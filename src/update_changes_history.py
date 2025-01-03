import os
import datetime as dt
import time as time
import pandas as pd
import numpy as np
import utils as ut
from multiprocess_pandas import applyparallel


def update(
        Datenstand: dt.datetime,
        BL: pd.DataFrame,
        LK: pd.DataFrame,
        oLc: pd.DataFrame,
        oLd: pd.DataFrame,
        oLr: pd.DataFrame,
        oLi: pd.DataFrame,
        oBc: pd.DataFrame,
        oBd: pd.DataFrame,
        oBr: pd.DataFrame,
        oBi: pd.DataFrame,
        oLDc: pd.DataFrame,
        oLDd: pd.DataFrame,
        oLDr: pd.DataFrame,
        oLDi: pd.DataFrame,
        oBDc: pd.DataFrame,
        oBDd: pd.DataFrame,
        oBDr: pd.DataFrame,
        oBDi: pd.DataFrame) -> list:
    
    # fields are
    # i = Id(Landkreis or Bundesland)
    # m = Meldedatum
    # c = cases
    # d = deaths
    # r = recovered
    # c7 = cases7d (cases7days)
    # i7 = incidence7d (incidence7days)

    # split LK
    Lc = LK[["i", "m", "c"]].copy()
    Ld = LK[["i", "m", "d"]].copy()
    Lr = LK[["i", "m", "r"]].copy()
    Li = LK[["i", "m", "c7", "i7"]].copy()
        
    # split BL
    Bc = BL[["i", "m", "c"]].copy()
    Bd = BL[["i", "m", "d"]].copy()
    Br = BL[["i", "m", "r"]].copy()
    Bi = BL[["i", "m", "c7", "i7"]].copy()
    
    # calculate diff data
        
    if not oLc.empty:
        LDc = ut.get_different_rows(oLc, Lc)
        LDc.set_index(["i", "m"], inplace=True, drop=False)
        oLc.set_index(["i","m"], inplace=True, drop=False)
        LDc["dc"] = LDc["c"] - oLc["c"]
        LDc["dc"] = LDc["dc"].fillna(LDc["c"]).astype(int)
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
        BDc["dc"] = BDc["dc"].fillna(BDc["c"]).astype(int)
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
            
    if not oLDd.empty:
        LDd = pd.concat([oLDd, LDd])
                    
    if not oLDr.empty:
        LDr = pd.concat([oLDr, LDr])
        
    if not oLDi.empty:
        LDi = pd.concat([oLDi, LDi])
            
    if not oBDc.empty:
        BDc = pd.concat([oBDc, BDc])
                    
    if not oBDd.empty:
        BDd = pd.concat([oBDd, BDd])
                
    if not oBDr.empty:
        BDr = pd.concat([oBDr, BDr])
                
    if not oBDi.empty:
        BDi = pd.concat([oBDi, BDi])
        
    return [Lc, Ld, Lr, Li, Bc, Bd, Br, Bi, LDc, LDd, LDr, LDi, BDc, BDd, BDr, BDi]

def update_mass(meta: dict) -> list:

    BV_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Bevoelkerung", "Bevoelkerung.csv")
    BV_dtypes = {"AGS": "str", "Altersgruppe": "str", "Name": "str", "GueltigAb": "object", "GueltigBis": "object",
                 "Einwohner": "Int32", "männlich": "Int32", "weiblich": "Int32"}
    CV_dtypes = {"IdLandkreis": "str", "NeuerFall": "Int32", "NeuerTodesfall": "Int32", "NeuGenesen": "Int32",
                 "AnzahlFall": "Int32", "AnzahlTodesfall": "Int32", "AnzahlGenesen": "Int32", "Meldedatum": "object"}

    # open bevoelkerung.csv
    BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
    BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

    # load covid latest from web
    Datenstand = dt.datetime.fromtimestamp(meta["modified"] / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
    featherPath = meta["filepath"].replace("csv", "feather")[:-3]
    if os.path.exists(featherPath):
        LK = ut.read_file(fn=featherPath)
    else:
        LK = pd.read_csv(meta["filepath"], engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
        LK = ut.squeeze_dataframe(LK)
        ut.write_file(df=LK, fn=featherPath, compression="lz4")

    
    # History
        
    LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(int)
    LK["AnzahlTodesfall"] = np.where(LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0).astype(int)
    LK["AnzahlGenesen"] = np.where(LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0).astype(int)
    LK.drop(["NeuerFall", "NeuerTodesfall", "NeuGenesen"], inplace=True, axis=1)
    LK.rename(columns={"IdLandkreis": "i", "Meldedatum": "m", "AnzahlFall": "c", "AnzahlTodesfall": "d", "AnzahlGenesen": "r"}, inplace=True)
    agg_key = {'c': 'sum', 'd': 'sum', 'r': 'sum'}
    LK = LK.groupby(by=["i", "m"], as_index=False, observed=True).agg(agg_key)
    
    LK["i"] = LK['i'].map('{:0>5}'.format)
    LK = ut.squeeze_dataframe(LK)
        
    BL = LK.copy()
    BL["i"] = BL["i"].str.slice(0,2)
    BL = BL.groupby(by=["i", "m"], as_index=False, observed=True).agg(agg_key)
    BL = ut.squeeze_dataframe(BL)
    
    agg_key = {'i': 'max', 'c': 'sum', 'd': 'sum', 'r': 'sum'}
    ID0 = BL.groupby(by=["m"], as_index=False, observed=True).agg(agg_key)
    ID0["i"] = "00"
    BL = pd.concat([BL, ID0])
        
    LK["m"] = LK["m"].astype(str)
    BL["m"] = BL["m"].astype(str)
    
    # fill dates for every region
    allDates = ut.squeeze_dataframe(pd.DataFrame(pd.date_range(end=(Datenstand - dt.timedelta(days=1)), start="2019-12-26").astype(str), columns=["m"]))
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

    BL = BL.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    BL["i7"] = (BL["c7"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)
    BL.reset_index(inplace=True, drop=True)
    
        
    LK = LK.groupby(["i"], observed=True).apply_parallel(ut.calc_incidence, progressbar=False)
    LK["i7"] = (LK["c7"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)
    LK.reset_index(inplace=True, drop=True)
            
    return [Datenstand, BL, LK]
