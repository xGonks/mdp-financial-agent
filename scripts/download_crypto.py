import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta


def descargar_historico_4y_horario(ticker: str) -> pd.DataFrame:
    """
    Descarga 4 años de datos horarios dividiendo en bloques
    porque Yahoo limita la cantidad de datos intraday.
    """

    end_date = datetime.today()
    start_date = end_date - timedelta(days=4 * 365)

    bloques = []
    fecha_inicio = start_date

    while fecha_inicio < end_date:
        fecha_fin = min(fecha_inicio + timedelta(days=365), end_date)

        print(f"   Descargando desde {fecha_inicio.date()} hasta {fecha_fin.date()}")

        df = yf.download(
            ticker,
            start=fecha_inicio.strftime("%Y-%m-%d"),
            end=fecha_fin.strftime("%Y-%m-%d"),
            interval="1h",
            progress=False,
            auto_adjust=False
        )

        if not df.empty:
            bloques.append(df)

        fecha_inicio = fecha_fin

    if not bloques:
        return pd.DataFrame()

    df_final = pd.concat(bloques)
    df_final = df_final[~df_final.index.duplicated(keep="first")]

    df_final.reset_index(inplace=True)
    df_final.rename(columns={"Datetime": "date"}, inplace=True)

    # Aplanar columnas si son MultiIndex
    if isinstance(df_final.columns, pd.MultiIndex):
        df_final.columns = df_final.columns.get_level_values(0)

    return df_final


def guardar_csv(df: pd.DataFrame, nombre_archivo: str, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{nombre_archivo}.csv"
    df.to_csv(file_path, index=False)
    print(f"✔ Guardado en: {file_path}")


def main():
    activos = {
        "bitcoin": "BTC-USD",
        "ethereum": "ETH-USD",
        "binancecoin": "BNB-USD",
        "solana": "SOL-USD"
    }

    base_path = Path(__file__).resolve().parent.parent
    output_dir = base_path / "data" / "raw"

    print("Descargando últimos 4 años con frecuencia horaria...\n")

    for nombre, ticker in activos.items():
        print(f"\nDescargando {nombre} ({ticker})")
        df = descargar_historico_4y_horario(ticker)

        if df.empty:
            print(f"❌ No se pudieron descargar datos para {nombre}")
        else:
            guardar_csv(df, nombre, output_dir)

    print("\n✅ Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()
