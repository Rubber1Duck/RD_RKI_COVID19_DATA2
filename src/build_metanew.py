import os, json, sys, requests
import datetime as dt
import time as time
from update_changes_history import update, update_mass

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
  ghrun = False
  initrun = False
  if len(sys.argv) != 4:
    raise ValueError('need exactly 3 arguments! (sdate, edate, type ghrun or initrun)')
  else:
    if sys.argv[3] == "ghrun":
      startdatum = sys.argv[1]
      enddatum = sys.argv[2]
      ghrun = True
    elif sys.argv[3] =="init":
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
  while sDatObj <= eDatObj:
    t1 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print (f"{aktuelleZeit} : running on {dt.datetime.strftime(sDatObj, '%Y-%m-%d')}")
    if ghrun:
      new_meta = build_meta(dt.datetime.strftime(sDatObj, format="%Y-%m-%d"))
      update(meta=new_meta, BL="", LK="", mode="auto" )
    elif initrun:
      new_meta = build_meta_init(dt.datetime.strftime(sDatObj, format="%Y-%m-%d"))
      update_mass(meta=new_meta)
    sDatObj += delta
    meta_path = os.path.join(base_path, "..", "dataStore", "meta", "meta.json")
    meta_path = os.path.normpath(meta_path)
    if os.path.exists(meta_path):
      os.remove(meta_path)
    with open(meta_path, "w", encoding="utf8") as json_file:
      json.dump(new_meta, json_file, ensure_ascii=False)
    t2 = time.time()
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : {round(t2 - t1, 5)} sec. for date {dt.datetime.strftime(sDatObj, '%Y-%m-%d')}")
  endTime = dt.datetime.now()
  aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
  print(f"{aktuelleZeit} : total python time: {endTime - startTime}")
  