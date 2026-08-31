import duckdb

con = duckdb.connect("dev.duckdb")

print(
    con.execute("SHOW TABLES").fetchall()
)
