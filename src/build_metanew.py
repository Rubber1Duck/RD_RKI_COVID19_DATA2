import os, json, sys, requests
import datetime as dt
import time as time
import pandas as pd
from update_changes_history import update, update_mass
import utils as ut

def build_meta(datum):
  BL_filename = "BL_BaseData.feather"
  LK_filename = "LK_BaseData.feather"
  base_url = "https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master/dataStore/"
  BL_url = base_url + "historychanges/" + BL_filename
  LK_url = base_url + "historychanges/" + LK_filename
  urlMeta = base_url + "meta/meta.json"
  # get meta.json
  meta_resp = requests.get(url=urlMeta, allow_redirects=True)
  metaObj = json.loads(meta_resp.content.decode(encoding="utf8"))
  if metaObj["version"] == datum:
    unix_timestamp = metaObj["modified"]
  else:
    date_time = dt.datetime.strptime(datum, "%Y-%m-%d")
    date_time_floored = dt.datetime.combine(date_time.date(), date_time.time().min).replace(tzinfo=dt.timezone.utc)
    unix_timestamp = int(dt.datetime.timestamp(date_time_floored)*1000)
  BL_size = requests.head(BL_url, allow_redirects=True).headers["content-length"]
  LK_size = requests.head(LK_url, allow_redirects=True).headers["content-length"]
    
  new_meta = {
    "publication_date": datum,
    "version": datum,
    "BL_filename": BL_filename,
    "BL_url": BL_url,
    "BL_size": BL_size,
    "LK_filename": LK_filename,
    "LK_url": LK_url,
    "LK_size": LK_size,
    "filename": "",
    "filepath": "",
    "filesize": "",
    "modified": unix_timestamp}
  
  return new_meta

def build_meta_init(datum):
  dataFileName ="RKI_COVID19_" + datum + ".csv.xz"
  base_path = os.path.dirname(os.path.abspath(__file__))
  data_path = os.path.join(base_path, "..","..", "RKIData", dataFileName)
  filesize = os.path.getsize(data_path)
  date_time = dt.datetime.strptime(datum, "%Y-%m-%d")
  date_time_floored = dt.datetime.combine(date_time.date(), date_time.time().min).replace(tzinfo=dt.timezone.utc)
  unix_timestamp = int(dt.datetime.timestamp(date_time_floored)*1000)
      
  new_meta = {
    "publication_date": datum,
    "version": datum,
    "BL_filename": "",
    "BL_url": "",
    "BL_size": "",
    "LK_filename": "",
    "LK_url": "",
    "LK_size": "",
    "filename": dataFileName,
    "filepath": data_path,
    "filesize": filesize,
    "modified": unix_timestamp}
  
  return new_meta

if __name__ == '__main__':
  start = dt.datetime.now()
  initrun = False
  if len(sys.argv) != 4:
    raise ValueError('need exactly 3 arguments! (sdate, edate, type ghrun or initrun)')
  else:
    if sys.argv[3] =="init":
      startdatum = sys.argv[1]
      enddatum = sys.argv[2]
      initrun = True
    else:
      raise ValueError('need exactly 3 arguments! (sdate, edate, type ghrun or initrun)')
  
  base_path = os.path.dirname(os.path.abspath(__file__))
  startTime = dt.datetime.now()
  sDatObj = dt.datetime.strptime(startdatum, "%Y-%m-%d")
  eDatObj = dt.datetime.strptime(enddatum, "%Y-%m-%d")
  delta = dt.timedelta(days=1)

  HC_dtp = {"i": "str", "m": "object", "c": "int64"}
  HD_dtp = {"i": "str", "m": "object", "d": "int64"}
  HR_dtp = {"i": "str", "m": "object", "r": "int64"}
  HI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float"}

  HCC_dtp = {"i": "str", "m": "object", "c": "int64", "dc": "int64", "cD": "object"}
  HCD_dtp = {"i": "str", "m": "object", "d": "int64", "cD": "object"}
  HCR_dtp = {"i": "str", "m": "object", "r": "int64", "cD": "object"}
  HCI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float", "cD": "object"}

  LKcasesFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "districts.json")
  LKdeathsFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "districts.json")
  LKrecoveredFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "districts.json")
  LKincidenceFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "districts.json")
    
  BLcasesFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "states.json")
  BLdeathsFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "states.json")
  BLrecoveredFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "states.json")
  BLincidenceFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "states.json")
  
  LKDiffCasesFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "districts_Diff.json")
  LKDiffDeathsFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "districts_Diff.json")
  LKDiffRecoveredFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "districts_Diff.json")
  LKDiffIncidenceFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "districts_Diff.json")

  BLDiffCasesFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "states_Diff.json")
  BLDiffDeathsFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "states_Diff.json")
  BLDiffRecoveredFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "states_Diff.json")
  BLDiffIncidenceFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "states_Diff.json")

  # if one path is present all others ust be present too!
  if os.path.exists(LKcasesFull):
    LKcasesHistory= ut.read_json(full_fn=LKcasesFull, dtype=HC_dtp)
    LKcasesHistory["c"] = LKcasesHistory["c"].astype("int64")
    LKdeathsHistory = ut.read_json(full_fn=LKdeathsFull, dtype=HD_dtp)
    LKdeathsHistory["d"] = LKdeathsHistory["d"].astype("int64")
    LKrecoveredHistory = ut.read_json(full_fn=LKrecoveredFull, dtype=HR_dtp)
    LKrecoveredHistory["r"] = LKrecoveredHistory["r"].astype("int64")
    LKincidenceHistory = ut.read_json(full_fn=LKincidenceFull, dtype=HI_dtp)
    LKincidenceHistory["c7"] = LKincidenceHistory["c7"].astype("int64")

    BLcasesHistory = ut.read_json(full_fn=BLcasesFull, dtype=HC_dtp)
    BLcasesHistory["c"] = BLcasesHistory["c"].astype("int64")
    BLdeathsHistory = ut.read_json(full_fn=BLdeathsFull, dtype=HD_dtp)
    BLdeathsHistory["d"] = BLdeathsHistory["d"].astype("int64")
    BLrecoveredHistory = ut.read_json(full_fn=BLrecoveredFull, dtype=HR_dtp)
    BLrecoveredHistory["r"] = BLrecoveredHistory["r"].astype("int64")
    BLincidenceHistory = ut.read_json(full_fn=BLincidenceFull, dtype=HI_dtp)
    BLincidenceHistory["c7"] = BLincidenceHistory["c7"].astype("int64")

    LKDiffCasesHistory = ut.read_json(full_fn=LKDiffCasesFull, dtype=HCC_dtp)
    LKDiffDeathsHistory = ut.read_json(full_fn=LKDiffDeathsFull, dtype=HCD_dtp)
    LKDiffRecoveredHistory = ut.read_json(full_fn=LKDiffRecoveredFull, dtype=HCR_dtp)
    LKDiffIncidenceHistory = ut.read_json(full_fn=LKDiffIncidenceFull, dtype=HCI_dtp)

    BLDiffCasesHistory = ut.read_json(full_fn=BLDiffCasesFull, dtype=HCC_dtp)
    BLDiffDeathsHistory = ut.read_json(full_fn=BLDiffDeathsFull, dtype=HCD_dtp)
    BLDiffRecoveredHistory = ut.read_json(full_fn=BLDiffRecoveredFull, dtype=HCR_dtp)
    BLDiffIncidenceHistory = ut.read_json(full_fn=BLDiffIncidenceFull, dtype=HCI_dtp)

  else:
    LKcasesHistory = pd.DataFrame()
    LKdeathsHistory = pd.DataFrame()
    LKrecoveredHistory = pd.DataFrame()
    LKincidenceHistory = pd.DataFrame()
    
    BLcasesHistory = pd.DataFrame()
    BLdeathsHistory = pd.DataFrame()
    BLrecoveredHistory = pd.DataFrame()
    BLincidenceHistory = pd.DataFrame()

    LKDiffCasesHistory = pd.DataFrame()
    LKDiffDeathsHistory = pd.DataFrame()
    LKDiffRecoveredHistory = pd.DataFrame()
    LKDiffIncidenceHistory = pd.DataFrame()

    BLDiffCasesHistory = pd.DataFrame()
    BLDiffDeathsHistory = pd.DataFrame()
    BLDiffRecoveredHistory = pd.DataFrame()
    BLDiffIncidenceHistory = pd.DataFrame()
  
  while sDatObj <= eDatObj:
    t1 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print (f"{aktuelleZeit} : running on {dt.datetime.strftime(sDatObj, '%Y-%m-%d')}")
    
    new_meta = build_meta_init(dt.datetime.strftime(sDatObj, format="%Y-%m-%d"))
    [BL, LK] = update_mass(meta=new_meta)
    [LKcasesHistory,
      LKdeathsHistory,
      LKrecoveredHistory,
      LKincidenceHistory,
      BLcasesHistory,
      BLdeathsHistory,
      BLrecoveredHistory,
      BLincidenceHistory,
      LKDiffCasesHistory,
      LKDiffDeathsHistory,
      LKDiffRecoveredHistory,
      LKDiffIncidenceHistory,
      BLDiffCasesHistory,
      BLDiffDeathsHistory,
      BLDiffRecoveredHistory,
      BLDiffIncidenceHistory] = update(
      new_meta,
      BL,
      LK,
      LKcasesHistory,
      LKdeathsHistory,
      LKrecoveredHistory,
      LKincidenceHistory,
      BLcasesHistory,
      BLdeathsHistory,
      BLrecoveredHistory,
      BLincidenceHistory,
      LKDiffCasesHistory,
      LKDiffDeathsHistory,
      LKDiffRecoveredHistory,
      LKDiffIncidenceHistory,
      BLDiffCasesHistory,
      BLDiffDeathsHistory,
      BLDiffRecoveredHistory,
      BLDiffIncidenceHistory)
    
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    if os.path.exists(meta_path):
      os.remove(meta_path)
    with open(meta_path, "w", encoding="utf8") as json_file:
      json.dump(new_meta, json_file, ensure_ascii=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : {round(t2 - t1, 5)} sec. for date {dt.datetime.strftime(sDatObj, '%Y-%m-%d')}")
    sDatObj += delta

  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : final store all Values", end="")
  t1 = time.time()

  ut.write_json(LKcasesHistory, LKcasesFull)
  ut.write_json(LKdeathsHistory, LKdeathsFull)
  ut.write_json(LKrecoveredHistory, LKrecoveredFull)
  ut.write_json(LKincidenceHistory, LKincidenceFull)
  
  ut.write_json(BLcasesHistory, BLcasesFull)
  ut.write_json(BLdeathsHistory, BLdeathsFull)
  ut.write_json(BLrecoveredHistory, BLrecoveredFull)
  ut.write_json(BLincidenceHistory, BLincidenceFull)

  ut.write_json(LKDiffCasesHistory, LKDiffCasesFull)
  ut.write_json(LKDiffDeathsHistory, LKDiffDeathsFull)
  ut.write_json(LKDiffRecoveredHistory, LKDiffRecoveredFull)
  ut.write_json(LKDiffIncidenceHistory, LKDiffIncidenceFull)

  ut.write_json(BLDiffCasesHistory, BLDiffCasesFull)
  ut.write_json(BLDiffDeathsHistory, BLDiffDeathsFull)
  ut.write_json(BLDiffRecoveredHistory, BLDiffRecoveredFull)
  ut.write_json(BLDiffIncidenceHistory, BLDiffIncidenceFull)

  t2 = time.time()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"Done in {round(t2 - t1, 5)} sec.")

  endTime = dt.datetime.now()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : total python time: {endTime - startTime}")
  