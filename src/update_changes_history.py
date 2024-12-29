import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
from multiprocess_pandas import applyparallel


def update(meta, BL, LK, oLc, oLd, oLr, oLi, oBc, oBd, oBr, oBi, oLDc, oLDd, oLDr, oLDi, oBDc, oBDd, oBDr, oBDi):
    
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(aktuelleZeit, ": prepare history data. ",end="")
    #t1 = time.time()
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
    Lc = LK[["i", "m", "c"]].copy()
    Lc["c"] = Lc["c"].astype("int64")
    Ld = LK[["i", "m", "d"]].copy()
    Ld["d"] = Ld["d"].astype("int64")
    Lr = LK[["i", "m", "r"]].copy()
    Lr["r"] = Lr["r"].astype("int64")
    Li = LK[["i", "m", "c7", "i7"]].copy()
    Li["c7"] = Li["c7"].astype("int64")
    
    # split BL
    Bc = BL[["i", "m", "c"]].copy()
    Bc["c"] = Bc["c"].astype("int64")
    Bd = BL[["i", "m", "d"]].copy()
    Bd["d"] = Bd["d"].astype("int64")
    Br = BL[["i", "m", "r"]].copy()
    Br["r"] = Br["r"].astype("int64")
    Bi = BL[["i", "m", "c7", "i7"]].copy()
    Bi["c7"] = Bi["c7"].astype("int64")    
    #t2 = time.time()
    #print(f"Done in {round((t2 - t1), 3)} sec.")
    
    # calculate diff data
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : calculating history difference. ",end="")
    #t1 = time.time()
    
    if not oLc.empty:
        LDc = ut.get_different_rows(oLc, Lc)
        LDc.set_index(["i", "m"], inplace=True, drop=False)
        oLc.set_index(["i","m"], inplace=True, drop=False)
        LDc["dc"] = LDc["c"] - oLc["c"]
        LDc["dc"] = LDc["dc"].fillna(LDc["c"])
        LDc.reset_index(inplace=True, drop=True)
        oLc.reset_index(inplace=True, drop=True)
    else:
        LDc = Lc.copy()
        LDc["dc"] = LDc["c"]
    
    if not oLd.empty:
        LDd = ut.get_different_rows(oLd, Ld)
    else:
        LDd = Ld.copy()
    
    if not oLr.empty:
        LDr = ut.get_different_rows(oLr, Lr)
    else:
        LDr = Lr.copy()
    
    if not oLi.empty:
        # dont compare float values
        oLiTemp = oLi[["i", "m", "c7"]].copy()
        LiTemp = Li[["i", "m", "c7"]].copy()
        LDi = ut.get_different_rows(oLiTemp, LiTemp)
        LDi.set_index(["i","m"], inplace=True, drop=False)
        Li.set_index(["i","m"], inplace=True, drop=False)
        LDi["i7"] = Li["i7"]
        LDi.reset_index(inplace=True, drop=True)
        Li.reset_index(inplace=True, drop=True)
        oLiTemp = pd.DataFrame()
        LiTemp = pd.DataFrame()
    else:
        LDi = Li.copy()

    if not oBc.empty:
        BDc = ut.get_different_rows(oBc, Bc)
        BDc.set_index(["i", "m"], inplace=True, drop=False)
        oBc.set_index(["i","m"], inplace=True, drop=False)
        BDc["dc"] = BDc["c"] - oBc["c"]
        BDc["dc"] = BDc["dc"].fillna(BDc["c"])
        BDc.reset_index(inplace=True, drop=True)
        oBc.reset_index(inplace=True, drop=True)
    else:
        BDc = Bc.copy()
        BDc["dc"] = BDc["c"]
    
    if not oBd.empty:
        BDd = ut.get_different_rows(oBd, Bd)
    else:
        BDd = Bd.copy()
    
    if not oBr.empty:
        BDr = ut.get_different_rows(oBr, Br)
    else:
        BDr = Br.copy()
    
    if not oBi.empty:
        oBiTemp = oBi[["i", "m", "c7"]].copy()
        BiTemp = Bi[["i", "m", "c7"]].copy()
        BDi = ut.get_different_rows(oBiTemp, BiTemp)
        BDi.set_index(["i","m"], inplace=True, drop=False)
        Bi.set_index(["i","m"], inplace=True, drop=False)
        BDi["i7"] = Bi["i7"]
        BDi.reset_index(inplace=True, drop=True)
        Bi.reset_index(inplace=True, drop=True)
        oBiTemp = pd.DataFrame()
        BiTemp = pd.DataFrame()
    else:
        BDi = Bi.copy()

    #t2 = time.time()
    #print(f"Done in {round((t2 - t1), 3)} sec.")
    
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : storing change history. ",end="")
    #t1 = time.time()
    ChangeDate = dt.datetime.strftime(Datenstand, "%Y-%m-%d")
    LDc["cD"] = ChangeDate
    LDd["cD"] = ChangeDate
    LDr["cD"] = ChangeDate
    LDi["cD"] = ChangeDate
    
    BDc["cD"] = ChangeDate
    BDd["cD"] = ChangeDate
    BDr["cD"] = ChangeDate
    BDi["cD"] = ChangeDate
    
    if not oLDc.empty:
        LDc = pd.concat([oLDc, LDc])
    LDc["dc"] = LDc["dc"].astype(int)
        
    if not oLDd.empty:
        LDd = pd.concat([oLDd, LDd])
                    
    if not oLDr.empty:
        LDr = pd.concat([oLDr, LDr])
        
    if not oLDi.empty:
        LDi = pd.concat([oLDi, LDi])
            
    if not oBDc.empty:
        BDc = pd.concat([oBDc, BDc])
    BDc["dc"] = BDc["dc"].astype(int)
                
    if not oBDd.empty:
        BDd = pd.concat([oBDd, BDd])
                
    if not oBDr.empty:
        BDr = pd.concat([oBDr, BDr])
                
    if not oBDi.empty:
        BDi = pd.concat([oBDi, BDi])
        
    #t2 = time.time()
    #print(f"Done in {round((t2 - t1), 3)} sec.")

    return [Lc, Ld, Lr, Li, Bc, Bd, Br, Bi, LDc, LDd, LDr, LDi, BDc, BDd, BDr, BDi]

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
    #t1 = time.time()
    #fileName = meta["filename"]
    #fileSize = int(meta["filesize"])
    fileNameFull = meta["filepath"]
    timeStamp = meta["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : load {fileName} (size: {fileSize} bytes). ", end="")
    
    LK = pd.read_csv(fileNameFull, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    
    # ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)
    
    #t2 = time.time()
    #print(f"Done in {round((t2 - t1), 3)} secs. {LK.shape[0]} rows. {round(LK.shape[0] / (t2 - t1), 0)} rows/sec.")
    
    # History
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : calculating history data. ", end="")
    #t1 = time.time()
    
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

    #t2 = time.time()
    #print(f"Done in {round((t2 - t1), 3)} secs.")
    
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : calculating BL incidence. {BL.shape[0]} rows. ",end="")
    #t1 = time.time()
    BL["m"] = BL["m"].astype(str)
    BL = BL.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    BL.reset_index(inplace=True, drop=True)
    BL.sort_values(["i", "m"], inplace=True, axis=0)
    BL.reset_index(inplace=True, drop=True)
    BL["i7"] = (BL["c7"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
    #t2 = time.time()
    #print(f"Done in {round(t2 - t1, 3)} sec.")
    
    #aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    #print(f"{aktuelleZeit} : calculating LK incidence. {LK.shape[0]} rows. ",end="")
    t1 = time.time()
    LK["m"] = LK["m"].astype(str)
    LK = LK.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    LK.reset_index(inplace=True, drop=True)
    LK.sort_values(["i", "m"], inplace=True, axis=0)
    LK.reset_index(inplace=True, drop=True)
    LK["i7"] = (LK["c7"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    #t2 = time.time()
    #print(f"Done in {round(t2-t1, 3)} sec.")
    
    return [BL, LK]
