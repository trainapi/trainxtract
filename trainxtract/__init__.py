import pandas as pd
import numpy as np
import os
import sqlite3
import click

BASE_NAME="JizdniRad2017_09-04"

def _make_path(in_dir, ext):
    return os.path.join(in_dir, "{0}.{1}".format(BASE_NAME, ext))

def create_help_db(in_dir, db_file) -> pd.DataFrame:
    if os.path.isfile(db_file):
        os.remove(db_file)
    constr = "sqlite:///{0}".format(db_file)
    stops = parse_stops(_make_path(in_dir, "db"))
    stops.to_sql("stops", constr)
    trains = parse_trains(_make_path(in_dir, "hlv"))
    trains.to_sql("trains", constr)
    routes = parse_routes(_make_path(in_dir, "trv"))
    routes.to_sql("routes", constr)
    companies = parse_companies(_make_path(in_dir, "dop"))
    companies.to_sql("companies", constr)
    trains2 = parse_trains2(_make_path(in_dir, "kdv"))
    trains2.to_sql("trains2", constr)

def create_final_db(in_db, out_db):
    if os.path.isfile(out_db):
        os.remove(out_db)
    constr_in = "sqlite:///{0}".format(in_db)
    constr_out = "sqlite:///{0}".format(out_db)

    sql_trains = "SELECT DISTINCT trains.number, trains.name, trains.company as company_id, trains2.code FROM trains JOIN trains2 ON trains.number = trains2.train_id"
    trains = pd.read_sql(sql_trains, constr_in)
    trains.to_sql("trains", constr_out, index=False)

    sql_companies = "SELECT company_id, _cislo as number, _cislo4 as number4, name, _zkratka AS short_name, country FROM companies"
    companies = pd.read_sql(sql_companies, constr_in)
    companies.to_sql("companies", constr_out, index=False)

    countries = pd.read_csv("countries.csv")
    countries.to_sql("countries", constr_out, index=False)

    sql_stops = "SELECT stop_id, name, country FROM stops"
    stops = pd.read_sql(sql_stops, constr_in)
    stops = stops.set_index(["stop_id", "country"])
    geo_csv = pd.read_csv("geo.csv").dropna()
    geo_csv = geo_csv.set_index(["stop_id", "country"])
    stops = stops.join(geo_csv[["lng", "lat", "google_name"]])
    stops = stops.reset_index()
    stops.to_sql("stops", constr_out, index=False)

    sql_routes = "SELECT DISTINCT trains.number AS train_id, routes.country, stop_id,  (CASE WHEN arr_hour IS NULL THEN NULL ELSE routes.arr_day END) as arr_day, arr_hour, arr_min, dep_day, dep_hour, dep_min, (CASE WHEN arr_hour IS NULL THEN 0 ELSE 1 END) AS is_stop FROM routes JOIN trains ON trains.number = routes.number"
    routes = pd.read_sql(sql_routes, constr_in)
    routes.to_sql("routes", constr_out, index=False)

def _parse_csv(path, columns):
    return pd.read_csv(path, encoding="cp1250", sep="|", names=columns)

def parse_stops(path) -> pd.DataFrame:
    return _parse_csv(path, ["country", "stop_id", "_cislo_obvodu", "name"])

def _extract_number(number):
    frags = number.split("/")
    return frags[0]

def parse_trains(path) -> pd.DataFrame:
    df = _parse_csv(path, ["number", "name", "_rezim", "company", "_urcvl", "updated_at", "valid_from", "valid_to", "country", "_evciskds", "_cena", "_produkt", "_novyvl"])
    df["number"] = df["number"].map(_extract_number)
    return df

def parse_routes(path) -> pd.DataFrame:
    df = _parse_csv(path, ["number", "country", "stop_id", "_ob", "_cislo", "_typ",
                           "_stkolprij",
                           "arr_day", "arr_hour", "arr_min", "arr_halfmin",
                           "travel_time_in_min",
                           "travel_time_halfmin", "dep_day", "dep_hour", "dep_min",
                           "dep_halfmin", "_stlkolodj",
                          "_trkolodj", "_extratkol", "_posodj",
                           "_vtp", "_ntp", "_zkbr", "_manipuluje", "_manzas",
                           "_manposta", "_celniodb", "is_request_stop",
                           "_pobnekdy", "_jennast", "_jenvyst", "_zastdd",
                           "_ohlasd3", "_vjobskol", "_pobytpul",
                           "_nezverzast", "_kalendar", "_kalpob", "_uvrat",
                           "_uvrathv", "_preprah", "_osa", "_rezervovano",
                           "_kalpp", "_hlnd", "_smerjizdy", "company_id",
                           "_tecko", "_odjcasprij", "_rucbr", "_relacnivl",
                           "_obsluhbl", "_lvjd", "_druhyvlakd3", "_vjkolobsvz",
                           "_vjkolobssv"])
    df["number"] = df["number"].map(_extract_number)
    return df

def parse_companies(path) -> pd.DataFrame:
    return _parse_csv(path, ["company_id", "_cislo", "name", "_zkratka", "country", "_cislo4"])

def parse_trains2(path) -> pd.DataFrame:
    df = _parse_csv(path, ["train_id", "country", "begin_country", "begin_stop", "_obz", "end_country", "end_stop", "_obdo",
                            "_kalendar", "code", "_porzstz", "_porzstdo"])
    df["train_id"] = df["train_id"].map(_extract_number)
    return df

@click.command()
@click.argument("in_dir")
@click.argument("db_file")
def run_app(in_dir, db_file):
    create_help_db(in_dir, "data_full2.db")
    create_final_db("data_full2.db", db_file)

@click.command()
@click.argument("in_dir")
@click.argument("db_file")
def run_help(in_dir, db_file):
    create_help_db(in_dir, db_file)

@click.command()
@click.argument("in_db")
@click.argument("out_db")
def run_final(db_in, db_out):
    create_final_db(in_db, out_db)
