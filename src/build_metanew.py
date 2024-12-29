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

  Hc = {"i": "str", "m": "object", "c": "int64"}
  Hd = {"i": "str", "m": "object", "d": "int64"}
  Hr = {"i": "str", "m": "object", "r": "int64"}
  Hi = {"i": "str", "m": "object", "c7": "int64", "i7": "float"}

  HCc = {"i": "str", "m": "object", "c": "int64", "dc": "int64", "cD": "object"}
  HCd = {"i": "str", "m": "object", "d": "int64", "cD": "object"}
  HCr = {"i": "str", "m": "object", "r": "int64", "cD": "object"}
  HCi = {"i": "str", "m": "object", "c7": "int64", "i7": "float", "cD": "object"}

  LcF = os.path.join(base_path, "..", "dataStore", "history", "cases", "districts.json")
  LdF = os.path.join(base_path, "..", "dataStore", "history", "deaths", "districts.json")
  LrF = os.path.join(base_path, "..", "dataStore", "history", "recovered", "districts.json")
  LiF = os.path.join(base_path, "..", "dataStore", "history", "incidence", "districts.json")
    
  BcF = os.path.join(base_path, "..", "dataStore", "history", "cases", "states.json")
  BdF = os.path.join(base_path, "..", "dataStore", "history", "deaths", "states.json")
  BrF = os.path.join(base_path, "..", "dataStore", "history", "recovered", "states.json")
  BiF = os.path.join(base_path, "..", "dataStore", "history", "incidence", "states.json")
  
  LDcF = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "districts_Diff.json")
  LDdF = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "districts_Diff.json")
  LDrF = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "districts_Diff.json")
  LDiF = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "districts_Diff.json")

  BDcF = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "states_Diff.json")
  BDdF = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "states_Diff.json")
  BDrF = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "states_Diff.json")
  BDiF = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "states_Diff.json")

  # if one path is present all others ust be present too!
  if os.path.exists(LcF):
    oLc = ut.read_json(full_fn=LcF, dtype=Hc)
    oLc["c"] = oLc["c"].astype("int64")
    oLd = ut.read_json(full_fn=LdF, dtype=Hd)
    oLd["d"] = oLd["d"].astype("int64")
    oLr = ut.read_json(full_fn=LrF, dtype=Hr)
    oLr["r"] = oLr["r"].astype("int64")
    oLi = ut.read_json(full_fn=LiF, dtype=Hi)
    oLi["c7"] = oLi["c7"].astype("int64")

    oBc = ut.read_json(full_fn=BcF, dtype=Hc)
    oBc["c"] = oBc["c"].astype("int64")
    oBd = ut.read_json(full_fn=BdF, dtype=Hd)
    oBd["d"] = oBd["d"].astype("int64")
    oBr = ut.read_json(full_fn=BrF, dtype=Hr)
    oBr["r"] = oBr["r"].astype("int64")
    oBi = ut.read_json(full_fn=BiF, dtype=Hi)
    oBi["c7"] = oBi["c7"].astype("int64")

    oLDc = ut.read_json(full_fn=LDcF, dtype=HCc)
    oLDd = ut.read_json(full_fn=LDdF, dtype=HCd)
    oLDr = ut.read_json(full_fn=LDrF, dtype=HCr)
    oLDi = ut.read_json(full_fn=LDiF, dtype=HCi)

    oBDc = ut.read_json(full_fn=BDcF, dtype=HCc)
    oBDd = ut.read_json(full_fn=BDdF, dtype=HCd)
    oBDr = ut.read_json(full_fn=BDrF, dtype=HCr)
    oBDi = ut.read_json(full_fn=BDiF, dtype=HCi)

  else:
    oLc = pd.DataFrame()
    oLd = pd.DataFrame()
    oLr = pd.DataFrame()
    oLi = pd.DataFrame()
    
    oBc = pd.DataFrame()
    oBd = pd.DataFrame()
    oBr = pd.DataFrame()
    oBi = pd.DataFrame()

    oLDc = pd.DataFrame()
    oLDd = pd.DataFrame()
    oLDr = pd.DataFrame()
    oLDi = pd.DataFrame()

    oBDc = pd.DataFrame()
    oBDd = pd.DataFrame()
    oBDr = pd.DataFrame()
    oBDi = pd.DataFrame()
  
  while sDatObj <= eDatObj:
    t1 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print (f"{aktuelleZeit} : running on {dt.datetime.strftime(sDatObj, '%Y-%m-%d')}", end="")
    
    new_meta = build_meta_init(dt.datetime.strftime(sDatObj, format="%Y-%m-%d"))
    [BL, LK] = update_mass(meta=new_meta)
    [oLc, oLd, oLr, oLi, oBc, oBd, oBr, oBi, oLDc, oLDd, oLDr, oLDi, oBDc, oBDd, oBDr, oBDi] = update(new_meta, BL, LK, oLc, oLd, oLr, oLi, oBc, oBd, oBr, oBi, oLDc, oLDd, oLDr, oLDi, oBDc, oBDd, oBDr, oBDi)
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    if os.path.exists(meta_path):
      os.remove(meta_path)
    with open(meta_path, "w", encoding="utf8") as json_file:
      json.dump(new_meta, json_file, ensure_ascii=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f" {round(t2 - t1, 5)} sec.")
    sDatObj += delta

  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : final store all Values", end="")
  t1 = time.time()

  ut.write_json(oLc, LcF)
  ut.write_json(oLd, LdF)
  ut.write_json(oLr, LrF)
  ut.write_json(oLi, LiF)
  
  ut.write_json(oBc, BcF)
  ut.write_json(oBd, BdF)
  ut.write_json(oBr, BrF)
  ut.write_json(oBi, BiF)

  oLDc.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oLDc, LDcF)
  oLDd.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oLDd, LDdF)
  oLDr.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oLDr, LDrF)
  oLDi.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oLDi, LDiF)

  oBDc.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oBDc, BDcF)
  oBDd.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oBDd, BDdF)
  oBDr.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oBDr, BDrF)
  oBDi.sort_values(by=["i", "m", "cD"], inplace=True)
  ut.write_json(oBDi, BDiF)

  t2 = time.time()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"Done in {round(t2 - t1, 5)} sec.")

  endTime = dt.datetime.now()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : total python time: {endTime - startTime}")
  