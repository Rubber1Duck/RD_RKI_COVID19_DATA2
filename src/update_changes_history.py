import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
import gc
from multiprocess_pandas import applyparallel


def update(meta, BL, LK, mode="auto"):
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": prepare history data. ",end="")
    t1 = time.time()
    HC_dtp = {"i": "str", "m": "object", "c": "int64"}
    HD_dtp = {"i": "str", "m": "object", "d": "int64"}
    HR_dtp = {"i": "str", "m": "object", "r": "int64"}
    HI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float"}

    HCC_dtp = {"i": "str", "m": "object", "c": "int64", "dc": "int64", "cD": "object"}
    HCD_dtp = {"i": "str", "m": "object", "d": "int64", "cD": "object"}
    HCR_dtp = {"i": "str", "m": "object", "r": "int64", "cD": "object"}
    HCI_dtp = {"i": "str", "m": "object", "c7": "int64", "i7": "float", "cD": "object"}
    

    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)

    base_path = os.path.dirname(os.path.abspath(__file__))
    if mode !="init":    
        BL = ut.read_file(meta["BL_url"])
        LK = ut.read_file(meta["LK_url"])
    
    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    LK.rename(columns={"IdLandkreis": "i", "Landkreis": "t", "Meldedatum": "m", "cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    BL.rename(columns={"IdBundesland": "i", "Bundesland": "t", "Meldedatum": "m","cases": "c", "deaths": "d", "recovered": "r", "cases7d": "c7", "incidence7d": "i7"}, inplace=True)
    
    # split LK
    LKcases = LK[["i", "m", "c"]].copy()
    LKcases["c"] = LKcases["c"].astype("int64")
    LKdeaths = LK[["i", "m", "d"]].copy()
    LKdeaths["d"] = LKdeaths["d"].astype("int64")
    LKrecovered = LK[["i", "m", "r"]].copy()
    LKrecovered["r"] = LKrecovered["r"].astype("int64")
    LKincidence = LK[["i", "m", "c7", "i7"]].copy()
    LKincidence["c7"] = LKincidence["c7"].astype("int64")
    
    # split BL
    BLcases = BL[["i", "m", "c"]].copy()
    BLcases["c"] = BLcases["c"].astype("int64")
    BLdeaths = BL[["i", "m", "d"]].copy()
    BLdeaths["d"] = BLdeaths["d"].astype("int64")
    BLrecovered = BL[["i", "m", "r"]].copy()
    BLrecovered["r"] = BLrecovered["r"].astype("int64")
    BLincidence = BL[["i", "m", "c7", "i7"]].copy()
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : Storeing history data. ",end="")
    t1=time.time()
    # store all files not compressed! will be done later
        
    LKcasesJsonFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "districts.json")
    LKdeathsJsonFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "districts.json")
    LKrecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "districts.json")
    LKincidenceJsonFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "districts.json")
    
    BLcasesJsonFull = os.path.join(base_path, "..", "dataStore", "history", "cases", "states.json")
    BLdeathsJsonFull = os.path.join(base_path, "..", "dataStore", "history", "deaths", "states.json")
    BLrecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "history", "recovered", "states.json")
    BLincidenceJsonFull = os.path.join(base_path, "..", "dataStore", "history", "incidence", "states.json")

    # read oldLK(cases, deaths, recovered, incidence) if old file exist
    # write new data 
    if os.path.exists(LKcasesJsonFull):
        oldLKcases = ut.read_json(full_fn=LKcasesJsonFull, dtype=HC_dtp)
        oldLKcases["c"] = oldLKcases["c"].astype("int64")
    ut.write_json(df=LKcases, full_fn=LKcasesJsonFull)
    
    if os.path.exists(LKdeathsJsonFull):
        oldLKdeaths = ut.read_json(full_fn=LKdeathsJsonFull, dtype=HD_dtp)
        oldLKdeaths["d"] = oldLKdeaths["d"].astype("int64")
    ut.write_json(df=LKdeaths, full_fn=LKdeathsJsonFull)
        
    if os.path.exists(LKrecoveredJsonFull):
        oldLKrecovered = ut.read_json(full_fn=LKrecoveredJsonFull, dtype=HR_dtp)
        oldLKrecovered["r"] = oldLKrecovered["r"].astype("int64")
    ut.write_json(df=LKrecovered, full_fn=LKrecoveredJsonFull)
        
    if os.path.exists(LKincidenceJsonFull):
        oldLKincidence = ut.read_json(full_fn=LKincidenceJsonFull, dtype=HI_dtp)
        oldLKincidence["c7"] = oldLKincidence["c7"].astype("int64")
    ut.write_json(df=LKincidence, full_fn=LKincidenceJsonFull)
        
    # read oldBL(cases, deaths, recovered, incidence) if old file exist
    # write new data
    if os.path.exists(BLcasesJsonFull):
        oldBLcases = ut.read_json(full_fn=BLcasesJsonFull, dtype=HC_dtp)
        oldBLcases["c"] = oldBLcases["c"].astype("int64")
    ut.write_json(df=BLcases, full_fn=BLcasesJsonFull)
       
    if os.path.exists(BLdeathsJsonFull):
        oldBLdeaths = ut.read_json(full_fn=BLdeathsJsonFull, dtype=HD_dtp)
        oldBLdeaths["d"] = oldBLdeaths["d"].astype("int64")
    ut.write_json(df=BLdeaths, full_fn=BLdeathsJsonFull)
       
    if os.path.exists(BLrecoveredJsonFull):
        oldBLrecovered = ut.read_json(full_fn=BLrecoveredJsonFull, dtype=HR_dtp)
        oldBLrecovered["r"] = oldBLrecovered["r"].astype("int64")
    ut.write_json(df=BLrecovered, full_fn=BLrecoveredJsonFull)
    
    if os.path.exists(BLincidenceJsonFull):
        oldBLincidence = ut.read_json(full_fn=BLincidenceJsonFull, dtype=HI_dtp)
        oldBLincidence["c7"] = oldBLincidence["c7"].astype("int64")
    ut.write_json(df=BLincidence, full_fn=BLincidenceJsonFull)
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    # calculate diff data
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating history difference. ",end="")
    t1 = time.time()
    
    try:
        LKDiffCases = ut.get_different_rows(oldLKcases, LKcases)
        LKDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldLKcases.set_index(["i","m"], inplace=True, drop=False)
        LKDiffCases["dc"] = LKDiffCases["c"] - oldLKcases["c"]
        LKDiffCases["dc"] = LKDiffCases["dc"].fillna(LKDiffCases["c"])
    except:
        LKDiffCases = LKcases.copy()
        LKDiffCases["dc"] = LKDiffCases["c"]
    
    try:
        LKDiffDeaths = ut.get_different_rows(oldLKdeaths, LKdeaths)
    except:
        LKDiffDeaths = LKdeaths.copy()
    
    try:
        LKDiffRecovered = ut.get_different_rows(oldLKrecovered, LKrecovered)
    except:
        LKDiffRecovered = LKrecovered.copy()
    
    try:
        # dont compare float values
        oldLKincidence.drop("i7", inplace=True, axis=1)
        temp = LKincidence.copy()
        LKincidence.drop("i7", inplace=True, axis=1)
        LKDiffIncidence = ut.get_different_rows(oldLKincidence, LKincidence)
        LKDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        LKDiffIncidence["i7"] = temp["i7"]
        LKDiffIncidence.reset_index(inplace=True, drop=True)
    except:
        LKDiffIncidence = LKincidence.copy()

    try:
        BLDiffCases = ut.get_different_rows(oldBLcases, BLcases)
        BLDiffCases.set_index(["i", "m"], inplace=True, drop=False)
        oldBLcases.set_index(["i","m"], inplace=True, drop=False)
        BLDiffCases["dc"] = BLDiffCases["c"] - oldBLcases["c"]
        BLDiffCases["dc"] = BLDiffCases["dc"].fillna(BLDiffCases["c"])
    except:
        BLDiffCases = BLcases.copy()
        BLDiffCases["dc"] = BLDiffCases["c"]
    
    try:
        BLDiffDeaths = ut.get_different_rows(oldBLdeaths, BLdeaths)
    except:
        BLDiffDeaths = BLdeaths.copy()
    
    try:
        BLDiffRecovered = ut.get_different_rows(oldBLrecovered, BLrecovered)
    except:
        BLDiffRecovered = BLrecovered.copy()
    
    try:
        oldBLincidence.drop("i7", inplace=True, axis=1)
        temp = BLincidence.copy()
        BLincidence.drop("i7", inplace=True, axis=1)
        BLDiffIncidence = ut.get_different_rows(oldBLincidence, BLincidence)
        BLDiffIncidence.set_index(["i","m"], inplace=True, drop=False)
        temp.set_index(["i","m"], inplace=True, drop=True)
        BLDiffIncidence["i7"] = temp["i7"]
        BLDiffIncidence.reset_index(inplace=True, drop=True)
    except:
        BLDiffIncidence = BLincidence.copy()

    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : storing change history. ",end="")
    t1 = time.time()
    ChangeDate = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LKDiffCases["cD"] = ChangeDate
    LKDiffDeaths["cD"] = ChangeDate
    LKDiffRecovered["cD"] = ChangeDate
    LKDiffIncidence["cD"] = ChangeDate
    
    BLDiffCases["cD"] = ChangeDate
    BLDiffDeaths["cD"] = ChangeDate
    BLDiffRecovered["cD"] = ChangeDate
    BLDiffIncidence["cD"] = ChangeDate
    
    LKDiffCasesJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "districts_Diff.json")
    LKDiffDeathsJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "districts_Diff.json")
    LKDiffRecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "districts_Diff.json")
    LKDiffIncidenceJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "districts_Diff.json")

    BLDiffCasesJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "cases", "states_Diff.json")
    BLDiffDeathsJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "deaths", "states_Diff.json")
    BLDiffRecoveredJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "recovered", "states_Diff.json")
    BLDiffIncidenceJsonFull = os.path.join(base_path, "..", "dataStore", "historychanges", "incidence", "states_Diff.json")
    
    if os.path.exists(LKDiffCasesJsonFull):
        LKoldDiffCases = ut.read_json(full_fn=LKDiffCasesJsonFull, dtype=HCC_dtp)
        LKoldDiffCases = LKoldDiffCases[LKoldDiffCases["cD"] != ChangeDate]
        LKDiffCases = pd.concat([LKoldDiffCases, LKDiffCases])
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffCases.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffCases, full_fn=LKDiffCasesJsonFull)
        
    if os.path.exists(LKDiffDeathsJsonFull):
        LKoldDiffDeaths = ut.read_json(full_fn=LKDiffDeathsJsonFull, dtype= HCD_dtp)
        LKoldDiffDeaths = LKoldDiffDeaths[LKoldDiffDeaths["cD"] != ChangeDate]
        LKDiffDeaths = pd.concat([LKoldDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffDeaths, full_fn=LKDiffDeathsJsonFull)
        
    if os.path.exists(LKDiffRecoveredJsonFull):
        LKoldDiffRecovered = ut.read_json(full_fn=LKDiffRecoveredJsonFull, dtype= HCR_dtp)
        LKoldDiffRecovered = LKoldDiffRecovered[LKoldDiffRecovered["cD"] != ChangeDate]
        LKDiffRecovered = pd.concat([LKoldDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffRecovered, full_fn=LKDiffRecoveredJsonFull)
        
    if os.path.exists(LKDiffIncidenceJsonFull):
        LKoldDiffIncidence = ut.read_json(full_fn=LKDiffIncidenceJsonFull, dtype= HCI_dtp)
        LKoldDiffIncidence = LKoldDiffIncidence[LKoldDiffIncidence["cD"] != ChangeDate]
        LKDiffIncidence = pd.concat([LKoldDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_json(df=LKDiffIncidence, full_fn=LKDiffIncidenceJsonFull)
    
    if os.path.exists(BLDiffCasesJsonFull):
        BLoldDiffCases = ut.read_json(full_fn=BLDiffCasesJsonFull, dtype=HCC_dtp)
        BLoldDiffCases = BLoldDiffCases[BLoldDiffCases["cD"] != ChangeDate]
        BLDiffCases = pd.concat([BLoldDiffCases, BLDiffCases])
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffCases.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffCases, full_fn=BLDiffCasesJsonFull)
        
    if os.path.exists(BLDiffDeathsJsonFull):
        BLoldDiffDeaths = ut.read_json(full_fn=BLDiffDeathsJsonFull, dtype= HCD_dtp)
        BLoldDiffDeaths = BLoldDiffDeaths[BLoldDiffDeaths["cD"] != ChangeDate]
        BLDiffDeaths = pd.concat([BLoldDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffDeaths.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffDeaths, full_fn=BLDiffDeathsJsonFull)
        
    if os.path.exists(BLDiffRecoveredJsonFull):
        BLoldDiffRecovered = ut.read_json(full_fn=BLDiffRecoveredJsonFull, dtype= HCR_dtp)
        BLoldDiffRecovered = BLoldDiffRecovered[BLoldDiffRecovered["cD"] != ChangeDate]
        BLDiffRecovered = pd.concat([BLoldDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffRecovered.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffRecovered, full_fn=BLDiffRecoveredJsonFull)
        
    if os.path.exists(BLDiffIncidenceJsonFull):
        BLoldDiffIncidence = ut.read_json(full_fn=BLDiffIncidenceJsonFull, dtype= HCI_dtp)
        BLoldDiffIncidence = BLoldDiffIncidence[BLoldDiffIncidence["cD"] != ChangeDate]
        BLDiffIncidence = pd.concat([BLoldDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    ut.write_json(df=BLDiffIncidence, full_fn=BLDiffIncidenceJsonFull)

    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")

    return

def update_mass(meta):
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
    BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object",
                 "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
    CV_dtypes = {"IdLandkreis": "str", "NeuerFall": "Int32", "NeuerTodesfall": "Int32", "NeuGenesen": "Int32",
                 "AnzahlFall": "Int32", "AnzahlTodesfall": "Int32", "AnzahlGenesen": "Int32", "Meldedatum": "object"}

    # open bevoelkerung.csv
    BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
    BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

    # load covid latest from web
    t1 = time.time()
    fileName = meta["filename"]
    fileSize = int(meta["filesize"])
    fileNameFull = meta["filepath"]
    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : load {fileName} (size: {fileSize} bytes). ", end="")
    
    LK = pd.read_csv(fileNameFull, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)
    
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} secs. {LK.shape[0]} rows. {round(LK.shape[0] / (t2 - t1), 0)} rows/sec.")
    
    # History
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating history data. ", end="")
    t1 = time.time()
    
    # used keylists
    key_list_LK = ["i", "m"]
    key_list_BL = ["i", "m"]
    key_list_ID0 = ["m"]

    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen"], inplace=True, axis=1)
    LK.rename(columns={"IdLandkreis": "i", "Meldedatum": "m", "AnzahlFall": "c", "AnzahlTodesfall": "d", "AnzahlGenesen": "r"}, inplace=True)
    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in LK.columns
        if c not in key_list_LK
    }
    LK = LK.groupby(by=key_list_LK, as_index=False, observed=True).agg(agg_key)
    
    LK["i"] = LK['i'].map('{:0>5}'.format)
    LK = ut.squeeze_dataframe(LK)
    LK.sort_values(by=key_list_LK, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    
    BL = LK.copy()
    BL["i"] = BL["i"].str.slice(0,2)
    BL = ut.squeeze_dataframe(BL)
    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in BL.columns
        if c not in key_list_BL
    }
    BL = BL.groupby(by=key_list_BL, as_index=False, observed=True).agg(agg_key)
    BL = ut.squeeze_dataframe(BL)
    BL.sort_values(by=key_list_BL, inplace=True)
    BL.reset_index(inplace=True, drop=True)

    agg_key = {
        c: "max" if c in ["i"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0
    }
    ID0 = BL.groupby(by=key_list_ID0, as_index=False, observed=True).agg(agg_key)
    ID0["i"] = "00"
    BL = pd.concat([ID0, BL])
    BL.sort_values(by=key_list_BL, inplace=True)
    BL.reset_index(inplace=True, drop=True)
    
    LK["m"] = LK["m"].astype(str)
    BL["m"] = BL["m"].astype(str)
    
    # fill dates for every region
    allDates = ut.squeeze_dataframe(pd.DataFrame(pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start="2020-01-01").astype(str), columns=["m"]))
    # add Einwohner
    BL_BV = BV[((BV["AGS"].isin(BL["i"])) & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["Altersgruppe"] == "A00+") & (BV["AGS"].str.len() == 2))].copy()
    BL_BV.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich", "Name"], inplace=True, axis=1)
    BL_BV.rename(columns={"AGS": "i"}, inplace=True)

    LK_BV = BV[((BV["AGS"].isin(LK["i"])) & (BV["GueltigAb"] <= Datenstand) & (BV["GueltigBis"] >= Datenstand) & (BV["Altersgruppe"] == "A00+") & (BV["AGS"].str.len() == 5))].copy()
    LK_BV.drop(["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"], inplace=True, axis=1)
    LK_BV.rename(columns={"AGS": "i", "Name": "Landkreis"}, inplace=True)

    BLDates = ut.squeeze_dataframe(BL_BV.merge(allDates, how="cross"))
    LKDates = ut.squeeze_dataframe(LK_BV.merge(allDates, how="cross"))
    
    BL = BL.merge(BLDates, how="right", on=["i", "m"])
    LK = LK.merge(LKDates, how="right", on=["i", "m"])
    LK = LK[LK["Landkreis"].notna()]
    LK.drop(["Landkreis"], inplace=True, axis=1)

    #fill nan with 0
    BL["c"] = BL["c"].fillna(0).astype(int)
    BL["d"] = BL["d"].fillna(0).astype(int)
    BL["r"] = BL["r"].fillna(0).astype(int)

    LK["c"] = LK["c"].fillna(0).astype(int)
    LK["d"] = LK["d"].fillna(0).astype(int)
    LK["r"] = LK["r"].fillna(0).astype(int)

    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} secs.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating BL incidence. {BL.shape[0]} rows. ",end="")
    t1 = time.time()
    BL["m"] = BL["m"].astype(str)
    BL = BL.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    BL.reset_index(inplace=True, drop=True)
    BL.sort_values(["i", "m"], inplace=True, axis=0)
    BL.reset_index(inplace=True, drop=True)
    BL["i7"] = (BL["c7"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
    t2 = time.time()
    print(f"Done in {round(t2 - t1, 3)} sec.")
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f"{aktuelleZeit} : calculating LK incidence. {LK.shape[0]} rows. ",end="")
    t1 = time.time()
    LK["m"] = LK["m"].astype(str)
    LK = LK.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    LK.reset_index(inplace=True, drop=True)
    LK.sort_values(["i", "m"], inplace=True, axis=0)
    LK.reset_index(inplace=True, drop=True)
    LK["i7"] = (LK["c7"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    t2 = time.time()
    print(f"Done in {round(t2-t1, 3)} sec.")
    
    update(meta=meta, BL=BL, LK=LK, mode="init")
    
    return
