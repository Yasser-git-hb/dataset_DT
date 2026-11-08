#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dataset HORAIRE : 2048 simulations EnergyPlus (8 angles x 4 fenetres x 4 portes), un fichier
Parquet par scenario (8760 lignes), sans perte d'information horaire.

Colonnes de chaque fichier
  scen_id, jour ("MM-JJ"), heure (1..24, heure de FIN : 1 = 00h-01h), angle,
  f_z2..f_z5 (fenetres), p_z2..p_z5 (portes) : 1 = ouvert, 0 = ferme,
  puis par zone : z<n>_T (air, C), z<n>_HR (humidite relative, %), z<n>_TRM (temp. radiante, C)

Modele : AirflowNetwork ajoute en memoire ; eclairage retire ; Timestep impose (1 par defaut).
Le fichier IDF source n'est jamais modifie.

Sorties (dataset_horaire/)
  horaire/S0001.parquet ...   un fichier par scenario (reprise automatique)
  scenarios.csv               id, angle, etats des ouvertures, code
  meteo_horaire.parquet       meteo exterieure horaire (commune a tous les scenarios)
  erreurs/, echecs.csv        diagnostics

Usage
  python generer_dataset_horaire.py --smoke     # 4 simulations de test (a faire en premier)
  python generer_dataset_horaire.py             # tout (reprend ou il s'est arrete)
  python generer_dataset_horaire.py --ids 6,8   # scenarios precis
Ctrl+C : annule les scenarios en attente ; relancer la meme commande pour reprendre.
"""
import argparse
import collections
import csv
import datetime
import glob
import importlib.util
import itertools
import os
import re
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(errors="replace")
except Exception:
    pass

# ==============================================================================
# 1. CONFIGURATION
# ==============================================================================
SOURCE_IDF = "maisonfinal.idf"  # ou --idf : votre IDF (avec ou sans eclairage)
WEATHER_EPW = "MAR_RK_Rabat-Sale.AP.601350_TMYx.2009-2023.epw"
ANGLES = [0, 45, 90, 135, 180, 225, 270, 315]
PAS_PAR_HEURE = 1          # Timestep impose dans l'IDF (--pas pour changer)
NB_LIGNES_ATTENDU = 8760   # une annee horaire
TIMEOUT_S = 3600

FENETRES = {"Z2": "Sub Surface 2", "Z3": "Sub Surface 11", "Z4": "Sub Surface 14", "Z5": "Sub Surface 15"}
PORTES = {"Z2": "Sub Surface 6", "Z3": "Sub Surface 17", "Z4": "Sub Surface 16", "Z5": "Sub Surface 7"}
OUVERTURES_PERMANENTES = ["Sub Surface 3", "Sub Surface 4"]  # toujours ouvertes : a verifier
SURFACES_FUITE = ["Surface 65", "Surface 8", "Surface 93", "Surface 90", "Surface 91", "Surface 41", "Surface 38"]
TAGS = ["F_Z2", "F_Z3", "F_Z4", "F_Z5", "P_Z2", "P_Z3", "P_Z4", "P_Z5"]  # ordre des bits

SORTIES = {  # variable EnergyPlus -> suffixe de colonne
    "Zone Mean Air Temperature": "T",
    "Zone Air Relative Humidity": "HR",
    "Zone Mean Radiant Temperature": "TRM",
}
ORDRE_SUFFIXES = ["T", "HR", "TRM"]

OUT = "dataset_horaire"
HOR_DIR = os.path.join(OUT, "horaire")
ERR_DIR = os.path.join(OUT, "erreurs")
TMP_DIR = os.path.join(OUT, "travail")
ECHECS_CSV = os.path.join(OUT, "echecs.csv")
HAS_PARQUET = importlib.util.find_spec("pyarrow") is not None
EXT = "parquet" if HAS_PARQUET else "csv.gz"


# ==============================================================================
# 2. MODELE IDF (lecture par objets)
# ==============================================================================
def lire_idf(chemin):
    with open(chemin, "r", encoding="utf-8", errors="ignore") as f:
        texte = re.sub(r"!.*", "", f.read())
    objets = []
    for bloc in texte.split(";"):
        champs = [c.strip() for c in bloc.split(",")]
        if champs and champs[0]:
            objets.append(champs)
    return objets


def ecrire_idf(objets):
    morceaux = []
    for o in objets:
        if len(o) == 1:
            morceaux.append(o[0] + ";\n")
        else:
            morceaux.append(o[0] + ",\n" + ",\n".join("  " + c for c in o[1:]) + ";\n")
    return "\n".join(morceaux)


def preparer_modele(objets, pas, retirer_eclairage):
    """Retire anciens AFN/sorties (et l'eclairage), coupe le dimensionnement, impose Timestep."""
    gardes, timestep_vu = [], False
    for o in objets:
        classe = o[0].upper()
        if (classe.startswith("AIRFLOWNETWORK:") or classe.startswith("OUTPUT:METER")
                or classe in ("OUTPUT:VARIABLE", "OUTPUT:SQLITE", "OUTPUT:TABLE:SUMMARYREPORTS")):
            continue
        if retirer_eclairage and (classe == "LIGHTS" or classe.startswith("DAYLIGHTING:")
                                  or classe.startswith("OUTPUT:ILLUMINANCEMAP")
                                  or classe.startswith("OUTPUTCONTROL:ILLUMINANCEMAP")):
            continue
        o = list(o)
        if classe == "TIMESTEP":
            o, timestep_vu = ["Timestep", str(pas)], True
        if classe == "SIMULATIONCONTROL" and len(o) > 5 and o[4].upper() == "YES":
            o[4] = "No"  # Run Simulation for Sizing Periods
        if classe == "BUILDING":
            while len(o) < 3:
                o.append("")
            o[2] = "{{ANGLE}}"  # North Axis
        gardes.append(o)
    if not timestep_vu:
        gardes.append(["Timestep", str(pas)])
    return gardes


def bloc_afn(zones):
    t = ["""AirflowNetwork:SimulationControl,
  AFNSimControl 1, MultizoneWithoutDistribution, SurfaceAverageCalculation, OpeningHeight, LowRise,
  3000, LinearInitializationMethod, 0.001, 1e-05, -0.5, 0, 1;"""]
    for z in zones:
        t.append(f"AirflowNetwork:MultiZone:Zone, {z}, NoVent, , 0, 0, 100, 0, 300000, , Standard, 10;")
    t.append("AirflowNetwork:MultiZone:ReferenceCrackConditions, AFN_Ref_Conditions, 20, 101325, 0;")
    t.append("AirflowNetwork:MultiZone:Surface:Crack, Fuite_Enveloppe, 0.0005, 0.65, AFN_Ref_Conditions;")
    t.append("AirflowNetwork:MultiZone:Component:SimpleOpening, Composant_Fenetre_Ext, 0.001, 0.65, 0.0001, 0.60;")
    t.append("AirflowNetwork:MultiZone:Component:SimpleOpening, Composant_Porte_Int, 0.002, 0.65, 0.0001, 0.65;")
    fin = ", , 0, 0, 100, 0, 300000;"
    for s in SURFACES_FUITE:
        t.append(f"AirflowNetwork:MultiZone:Surface, {s}, Fuite_Enveloppe, , 1, NoVent{fin}")
    for s in OUVERTURES_PERMANENTES:
        t.append(f"AirflowNetwork:MultiZone:Surface, {s}, Composant_Fenetre_Ext, , 1, Constant{fin}")
    for zone, s in FENETRES.items():
        t.append(f"AirflowNetwork:MultiZone:Surface, {s}, Composant_Fenetre_Ext, , 1, {{{{F_{zone}}}}}{fin}")
    for zone, s in PORTES.items():
        t.append(f"AirflowNetwork:MultiZone:Surface, {s}, Composant_Porte_Int, , 1, {{{{P_{zone}}}}}{fin}")
    for v in SORTIES:
        t.append(f"Output:Variable, *, {v}, Hourly;")
    return "\n".join(t) + "\n"


def idf_scenario(modele, angle, bits):
    t = modele.replace("{{ANGLE}}", str(angle))
    for tag, bit in zip(TAGS, bits):
        t = t.replace("{{" + tag + "}}", "Constant" if bit else "NoVent")
    return t


def verifier_modele(objets):
    """Un nom de surface absent de l'IDF ferait echouer TOUS les scenarios."""
    noms = {o[1].upper() for o in objets if len(o) > 1
            and re.search(r"SURFACE|WALL|ROOF|FLOOR|CEILING|WINDOW|DOOR", o[0], re.I)
            and not re.match(r"(WINDOWMATERIAL|MATERIAL|CONSTRUCTION|WINDOWPROPERTY|SHADINGCONTROL)", o[0], re.I)}
    attendus = SURFACES_FUITE + OUVERTURES_PERMANENTES + list(FENETRES.values()) + list(PORTES.values())
    return [n for n in attendus if n.upper() not in noms]


# ==============================================================================
# 3. TABLES ANNEXES
# ==============================================================================
def construire_scenarios():
    combos = ((a, b) for a in ANGLES for b in itertools.product([0, 1], repeat=8))
    return [(i, a, b) for i, (a, b) in enumerate(combos, 1)]


def nom_scenario(sid, angle, bits):
    return f"S{sid:04d}_A{angle:03d}_B{''.join(map(str, bits))}"


def ecrire_table_scenarios(scenarios):
    with open(os.path.join(OUT, "scenarios.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["scen_id", "angle"] + [t.lower() for t in TAGS] + ["code"])
        for sid, angle, bits in scenarios:
            w.writerow([sid, angle] + list(bits) + [nom_scenario(sid, angle, bits)])


def meteo_horaire(epw):
    """Meteo exterieure horaire (identique pour tous les scenarios) : jointure sur (jour, heure)."""
    d = pd.read_csv(epw, skiprows=8, header=None, usecols=[1, 2, 3, 6, 8, 13, 14, 15, 20, 21, 22],
                    names=["mois", "jm", "heure", "T_ext", "RH_ext", "GHI", "DNI", "DHI",
                           "vent_dir", "vent_vit", "nuages"])
    d.insert(0, "jour", d["mois"].map("{:02d}".format) + "-" + d["jm"].map("{:02d}".format))
    d["heure"] = d["heure"].astype("int8")
    d = d.drop(columns=["mois", "jm"])
    return d.astype({c: "float32" for c in d.columns[2:]})


# ==============================================================================
# 4. TRANSFORMATION D'UNE SIMULATION EN TABLE HORAIRE
# ==============================================================================
RE_COL = re.compile(r"^(?P<cle>[^:]+):(?P<var>.+?)\s*\[(?P<unite>[^\]]*)\]\((?P<freq>[^)]*)\)\s*$")


def nom_court(cle):
    m = re.search(r"thermal zone (\d+)", cle, re.I)
    return f"z{m.group(1)}" if m else re.sub(r"\W+", "_", cle.strip().lower()).strip("_")


def cle_tri(col):
    m = re.match(r"z(\d+)_(\w+)", col)
    return (int(m.group(1)), ORDRE_SUFFIXES.index(m.group(2))) if m else (999, 0)


def transformer(df, sid, angle, bits):
    ex = df.iloc[:, 0].astype(str).str.extract(r"(?<!\d)(\d{2})/(\d{2})(?!\d)\D+(\d{1,2}):\d{2}")
    if ex.isna().any().any():
        raise ValueError("colonne Date/Time illisible dans eplusout.csv")
    out = pd.DataFrame({"jour": (ex[0] + "-" + ex[1]).values, "heure": ex[2].astype("int8").values})
    series = {}
    for col in df.columns[1:]:
        m = RE_COL.match(col.strip())
        if m and m["var"].strip() in SORTIES:
            series[f"{nom_court(m['cle'])}_{SORTIES[m['var'].strip()]}"] = \
                pd.to_numeric(df[col], errors="coerce").astype("float32").values
    if not series:
        raise ValueError("aucune colonne de sortie reconnue (noms de variables differents ?)")
    for c in sorted(series, key=cle_tri):
        out[c] = series[c]
    if len(out) != NB_LIGNES_ATTENDU:
        raise ValueError(f"{len(out)} lignes au lieu de {NB_LIGNES_ATTENDU} : verifiez le RunPeriod de l'IDF")
    entete = {"scen_id": np.int16(sid), "angle": np.int16(angle)}
    entete.update({t.lower(): np.int8(b) for t, b in zip(TAGS, bits)})
    for k, v in entete.items():
        out[k] = v
    cols = ["scen_id", "jour", "heure", "angle"] + [t.lower() for t in TAGS]
    return out[cols + [c for c in out.columns if c not in cols]]


# ==============================================================================
# 5. ENTREES / SORTIES
# ==============================================================================
def ecrire_table(df, chemin_sans_ext):
    final = f"{chemin_sans_ext}.{EXT}"
    tmp = final + ".tmp"  # ecriture atomique : fichier present = scenario termine
    if HAS_PARQUET:
        df.to_parquet(tmp, index=False, compression="zstd")
    else:
        df.to_csv(tmp, index=False, compression="gzip")
    os.replace(tmp, final)


def base_resultat(sid):
    return os.path.join(HOR_DIR, f"S{sid:04d}")


def chemin_resultat(sid):
    return f"{base_resultat(sid)}.{EXT}"


def extraire_severes(chemin, n=5):
    if not os.path.exists(chemin):
        return ""
    with open(chemin, "r", encoding="utf-8", errors="ignore") as f:
        lignes = f.read().splitlines()
    blocs, fin = [], -1
    for i, l in enumerate(lignes):
        if i >= fin and re.search(r"\*\*\s*(Severe|Fatal)\s*\*\*", l):
            blocs.append("\n".join(x.strip() for x in lignes[i:i + 4]))
            fin = i + 4
    return "\n".join(blocs[:n])


def trouver_energyplus(arg=None):
    cands = [c for c in (arg, os.environ.get("ENERGYPLUS_EXE")) if c and os.path.exists(c)]
    cands += sorted(glob.glob(r"C:\EnergyPlus*\energyplus.exe"))
    cands += sorted(glob.glob(r"C:\openstudio*\EnergyPlus\energyplus.exe"))
    if not cands:
        sys.exit("energyplus.exe introuvable : utilisez --ep CHEMIN ou la variable ENERGYPLUS_EXE.")
    return cands[0]


# ==============================================================================
# 6. UNE SIMULATION
# ==============================================================================
def simuler(sc, ctx):
    sid, angle, bits = sc
    nom = nom_scenario(sid, angle, bits)
    dossier = os.path.abspath(os.path.join(TMP_DIR, nom))
    os.makedirs(dossier, exist_ok=True)
    chemin_idf = os.path.join(dossier, nom + ".idf")
    t0 = time.time()
    try:
        with open(chemin_idf, "w", encoding="utf-8") as f:
            f.write(idf_scenario(ctx["modele"], angle, bits))
        try:
            # cwd=dossier : ReadVarsESO ecrit readvars.audit dans le repertoire COURANT ; partage
            # entre simulations paralleles, il provoquait "remove: ... readvars.audit".
            rc = subprocess.run([ctx["exe"], "-w", ctx["epw"], "-d", dossier, "-r", chemin_idf],
                                capture_output=True, text=True, timeout=TIMEOUT_S, cwd=dossier).returncode
        except subprocess.TimeoutExpired:
            rc = -1
        csv_path = os.path.join(dossier, "eplusout.csv")
        if rc != 0 or not os.path.exists(csv_path):
            err = extraire_severes(os.path.join(dossier, "eplusout.err")) or f"code retour {rc} (voir erreurs/)"
            for fichier in ("eplusout.err", nom + ".idf"):
                if os.path.exists(os.path.join(dossier, fichier)):
                    cible = fichier if fichier.startswith(nom) else f"{nom}_{fichier}"
                    shutil.copy(os.path.join(dossier, fichier), os.path.join(ERR_DIR, cible))
            return {"id": sid, "nom": nom, "ok": False, "erreur": err, "duree": time.time() - t0}
        res = transformer(pd.read_csv(csv_path), sid, angle, bits)
        ecrire_table(res, base_resultat(sid))
        return {"id": sid, "nom": nom, "ok": True, "duree": time.time() - t0,
                "lignes": len(res), "colonnes": list(res.columns)}
    except Exception as e:  # une erreur de post-traitement ne doit pas tuer tout le lot
        return {"id": sid, "nom": nom, "ok": False, "erreur": f"{type(e).__name__}: {e}", "duree": time.time() - t0}
    finally:
        shutil.rmtree(dossier, ignore_errors=True)


def noter_echec(r):
    nouveau = not os.path.exists(ECHECS_CSV)
    with open(ECHECS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nouveau:
            w.writerow(["scen_id", "nom", "erreur"])
        w.writerow([r["id"], r["nom"], r["erreur"]])


# ==============================================================================
# 7. PROGRAMME PRINCIPAL
# ==============================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--idf", default=SOURCE_IDF)
    ap.add_argument("--epw", default=WEATHER_EPW)
    ap.add_argument("--pas", type=int, default=PAS_PAR_HEURE, help="Timestep (pas de calcul par heure)")
    ap.add_argument("--smoke", action="store_true", help="4 simulations de test, erreurs affichees")
    ap.add_argument("--ids", help="identifiants separes par des virgules")
    ap.add_argument("--limit", type=int, help="nombre maximal de scenarios a lancer")
    ap.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 4))
    ap.add_argument("--garder-eclairage", action="store_true", help="ne pas retirer Lights/Daylighting")
    ap.add_argument("--ignorer-verif", action="store_true")
    ap.add_argument("--ep", help="chemin de energyplus.exe")
    args = ap.parse_args()

    for d in (OUT, HOR_DIR, ERR_DIR):
        os.makedirs(d, exist_ok=True)
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR, exist_ok=True)

    exe = trouver_energyplus(args.ep)
    print(f"EnergyPlus : {exe}")
    objets = lire_idf(args.idf)
    zones = [o[1] for o in objets if o[0].upper() == "ZONE" and len(o) > 1]
    print(f"Zones ({len(zones)}) : {zones}")
    print(f"Timestep impose : {args.pas} par heure")
    gains = collections.Counter(o[0] for o in objets if o[0].upper() in
                                ("PEOPLE", "LIGHTS", "ELECTRICEQUIPMENT", "GASEQUIPMENT", "OTHEREQUIPMENT"))
    print(f"Apports internes presents dans l'IDF : {dict(gains) or 'aucun'}"
          + ("" if args.garder_eclairage else " (Lights sera retire)"))
    manquants = verifier_modele(objets)
    if manquants:
        print(f"ATTENTION : surfaces absentes de l'IDF : {manquants}")
        if not args.ignorer_verif:
            sys.exit("Corrigez les noms (FENETRES/PORTES/SURFACES_FUITE) ou relancez avec --ignorer-verif.")

    modele = ecrire_idf(preparer_modele(objets, args.pas, not args.garder_eclairage)) + "\n" + bloc_afn(zones)
    with open(os.path.join(OUT, "maison_afn_exemple.idf"), "w", encoding="utf-8") as f:
        f.write(idf_scenario(modele, 0, (0,) * 8))  # pour inspection manuelle
    ctx = {"modele": modele, "exe": os.path.abspath(exe), "epw": os.path.abspath(args.epw)}

    tous = construire_scenarios()
    ecrire_table_scenarios(tous)
    ecrire_table(meteo_horaire(args.epw), os.path.join(OUT, "meteo_horaire"))

    if args.smoke:
        ids = {1, 256, 6, 8}  # tout ferme, tout ouvert, et les deux scenarios qui avaient echoue
        a_faire, workers = [s for s in tous if s[0] in ids], 4
    else:
        a_faire = tous
        if args.ids:
            wanted = {int(x) for x in args.ids.split(",")}
            a_faire = [s for s in tous if s[0] in wanted]
        a_faire = [s for s in a_faire if not os.path.exists(chemin_resultat(s[0]))]
        if args.limit:
            a_faire = a_faire[:args.limit]
        workers = args.workers
    print(f"{len(a_faire)} scenario(s) a simuler ({workers} en parallele).")
    if not a_faire:
        print("Rien a faire : tous les fichiers existent deja.")
        return

    n_ok = n_ko = 0
    t0 = time.time()
    abandon = interrompu = False
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(simuler, s, ctx) for s in a_faire]
        try:
            for fut in as_completed(futures):
                r = fut.result()
                if r["ok"]:
                    n_ok += 1
                    print(f"[{n_ok + n_ko}/{len(a_faire)}] OK     {r['nom']} ({r['duree']:.0f}s, {r['lignes']} lignes)")
                    if args.smoke:
                        print(f"        colonnes : {r['colonnes']}")
                else:
                    n_ko += 1
                    noter_echec(r)
                    print(f"[{n_ok + n_ko}/{len(a_faire)}] ECHEC  {r['nom']} ({r['duree']:.0f}s)")
                    if args.smoke or n_ko <= 2:
                        print("        " + r["erreur"].replace("\n", "\n        "))
                if n_ok + n_ko >= 3 and not args.smoke:
                    eta = (time.time() - t0) / (n_ok + n_ko) * (len(a_faire) - n_ok - n_ko)
                    print(f"        temps restant estime : {datetime.timedelta(seconds=int(eta))}")
                if n_ok == 0 and n_ko >= max(3, workers) and not args.smoke:
                    abandon = True
                    ex.shutdown(wait=False, cancel_futures=True)
                    break
        except KeyboardInterrupt:
            interrompu = True
            print("\nCtrl+C : scenarios en attente annules ; attente de la fin des simulations en cours.")
            ex.shutdown(wait=False, cancel_futures=True)
    if interrompu:
        print("Interrompu. Relancez la meme commande : les scenarios termines sont conserves.")
        return
    if abandon:
        print("\nLes premieres simulations ont TOUTES echoue : probleme systematique (modele ou environnement).\n"
              "Voir erreurs/*_eplusout.err et envoyez les lignes 'Severe'.")
        return
    print(f"\nTermine : {n_ok} OK, {n_ko} echecs ({datetime.timedelta(seconds=int(time.time() - t0))}).")


if __name__ == "__main__":
    main()
