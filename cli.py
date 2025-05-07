import typer
import yaml
import sqlite3
from pathlib import Path

app = typer.Typer()

@app.command()
def add(schema_path: str):
    if not schema_path.endswith("_schema.yaml"):
        print("Bitte nur Schema-Dateien mit '_schema.yaml' hinzufügen.")
        raise typer.Exit(code=1)

    # Lade Schema-Datei
    with open(schema_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Datenbankpfad
    db_path = Path("data") / f"{config['table']}.db"
    
    # Verbindung zur Datenbank herstellen
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Spalten-Definitionen erstellen (OHNE CHECK-CONSTRAINTS)
    columns = []
    for field in config["fields"]:
        column_def = f"{field['name']} {field['type']}"
        if field.get("unique"):
            column_def += " UNIQUE"
        columns.append(column_def)
    
    # SQL-Befehl zum Erstellen der Tabelle
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config['table']} (
            {', '.join(columns)}
        )
    """
    
    # Debug-Ausgabe des SQL-Befehls
    print("ℹ️ Generierter SQL-Befehl:", create_table_sql)
    
    # Tabelle erstellen
    cursor.execute(create_table_sql)
    conn.commit()
    conn.close()
    
    print(f"✅ Tabelle {config['table']} in {db_path} erfolgreich erstellt!")

@app.command()
def hello():
    """Ein Beispielbefehl"""
    print("Hallo von deinem CLI!")

if __name__ == "__main__":
    app()


