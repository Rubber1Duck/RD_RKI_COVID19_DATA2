import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
from multiprocess_pandas import applyparallel


def update(
        meta,
        BL,
        LK,
        oldLKcases,
        oldLKdeaths,
        oldLKrecovered,
        oldLKincidence,
        oldBLcases,
        oldBLdeaths,
        oldBLrecovered,
        oldBLincidence,
        oldLKDiffCases,
        oldLKDiffDeaths,
        oldLKDiffRecovered,
        oldLKDiffIncidence,
        oldBLDiffCases,
        oldBLDiffDeaths,
        oldBLDiffRecovered,
        oldBLDiffIncidence):
    
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": prepare history data. ",end="")
    t1 = time.time()
    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)

    # for smaler files rename fields
    # i = Id(Landkreis or Bundesland)
    # t = Name of Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

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
    
    if not oldLKDiffCases.empty:
        LKDiffCases = pd.concat([oldLKDiffCases, LKDiffCases])
    LKDiffCases["dc"] = LKDiffCases["dc"].astype(int)
    LKDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffCases.reset_index(inplace=True, drop=True)
    
    if not oldLKDiffDeaths.empty:
        LKDiffDeaths = pd.concat([oldLKDiffDeaths, LKDiffDeaths])
    LKDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffDeaths.reset_index(inplace=True, drop=True)
                
    if not oldLKDiffRecovered.empty:
        LKDiffRecovered = pd.concat([oldLKDiffRecovered, LKDiffRecovered])
    LKDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffRecovered.reset_index(inplace=True, drop=True)
    
    if not oldLKDiffIncidence.empty:
        LKDiffIncidence = pd.concat([oldLKDiffIncidence, LKDiffIncidence])
    LKDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    LKDiffIncidence.reset_index(inplace=True, drop=True)
        
    if not oldBLDiffCases.empty:
        BLDiffCases = pd.concat([oldBLDiffCases, BLDiffCases])
    BLDiffCases["dc"] = BLDiffCases["dc"].astype(int)
    BLDiffCases.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffCases.reset_index(inplace=True, drop=True)
            
    if not oldBLDiffDeaths.empty:
        BLDiffDeaths = pd.concat([oldBLDiffDeaths, BLDiffDeaths])
    BLDiffDeaths.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffDeaths.reset_index(inplace=True, drop=True)
            
    if not oldBLDiffRecovered.empty:
        BLDiffRecovered = pd.concat([oldBLDiffRecovered, BLDiffRecovered])
    BLDiffRecovered.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffRecovered.reset_index(inplace=True, drop=True)
            
    if not oldBLDiffIncidence.empty:
        BLDiffIncidence = pd.concat([oldBLDiffIncidence, BLDiffIncidence])
    BLDiffIncidence.sort_values(by=["i", "m", "cD"], inplace=True)
    BLDiffIncidence.reset_index(inplace=True, drop=True)
    
    t2 = time.time()
    print(f"Done in {round((t2 - t1), 3)} sec.")

    return [
        LKcases,
        LKdeaths,
        LKrecovered,
        LKincidence,
        BLcases,
        BLdeaths,
        BLrecovered,
        BLincidence,
        LKDiffCases,
        LKDiffDeaths,
        LKDiffRecovered,
        LKDiffIncidence,
        BLDiffCases,
        BLDiffDeaths,
        BLDiffRecovered,
        BLDiffIncidence]

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
    
    return [BL, LK]
