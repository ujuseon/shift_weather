import requests
import pandas as pd

API_URL = "https://api.open-meteo.com/v1/forecast?latitude=55.0344&longitude=82.9434&daily=sunrise,sunset,daylight_duration&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,temperature_80m,temperature_120m,wind_speed_10m,wind_speed_80m,wind_direction_10m,wind_direction_80m,visibility,evapotranspiration,weather_code,soil_temperature_0cm,soil_temperature_6cm,rain,showers,snowfall&timezone=auto&timeformat=iso8601&wind_speed_unit=kn&temperature_unit=fahrenheit&precipitation_unit=inch&start_date=2025-06-16&end_date=2025-06-30"

# Функция получения данных по URL
def fetch_data(url):
    # Запрос данных по API Open-Meteo
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

# Функция преобразования дюймов в миллиметры
def inch_to_mm(value):
    return round(value * 25.4, 5)

# Функция для преобразования температуры по Фаренгейту в температуру в Цельсиях
def fahrenheit_to_celsius(degrees):
    return round((degrees - 32) * 5 / 9, 5)

# Функция для преобразования узлов в метры в секунду
def knots_to_mps(knots):
    return round(knots * 0.514444, 5)

# Функция для преобразования футов в метры
def ft_to_m(ft):
    return round(ft * 0.3048, 5)

# Преобразование данных
def transform_data(data):

    # Создание DataFrame для hourly данных
    hourly_data = data.get('hourly', {})
    df_hourly = pd.DataFrame(hourly_data)

    # Создание DataFrame для daily данных
    daily_data = data.get('daily', {})
    df_daily = pd.DataFrame(daily_data)

    # Переименование столбцов
    df_daily.rename(columns={'sunrise': 'sunrise_iso', 'sunset': 'sunset_iso', 'time': 'date'}, inplace=True)
    df_hourly.rename(columns={'time': 'datetime'}, inplace=True)

    # Преобразование типов
    df_daily['sunrise_dt'] = pd.to_datetime(df_daily['sunrise_iso'])
    df_daily['sunset_dt'] = pd.to_datetime(df_daily['sunset_iso'])
    df_daily['date'] = pd.to_datetime(df_daily['date']).dt.date
    df_daily['daylight_hours'] = (df_daily['sunset_dt'] - df_daily['sunrise_dt']).dt.total_seconds() / 3600

    df_hourly['datetime'] = pd.to_datetime(df_hourly['datetime'])
    df_hourly['date'] = df_hourly['datetime'].dt.date

    # Преобразование осадков
    precipitation_columns = ['rain', 'showers', 'snowfall']
    for col in precipitation_columns:
        if col in df_hourly.columns and df_hourly[col].notnull().any():
            df_hourly[f'{col}_mm'] = df_hourly[col].apply(inch_to_mm)
        else:
            df_hourly[f'{col}_mm'] = None

    # Преобразование температур
    temperature_columns = [
        'temperature_2m', 'dew_point_2m', 'apparent_temperature',
        'temperature_80m', 'temperature_120m',
        'soil_temperature_0cm', 'soil_temperature_6cm'
    ]
    for col in temperature_columns:
        if col in df_hourly.columns and df_hourly[col].notnull().all():
            df_hourly[f'{col}_celsius'] = df_hourly[col].apply(fahrenheit_to_celsius)
        else:
            df_hourly[f'{col}_celsius'] = None

    # Преобразование скорости ветра
    wind_columns = ['wind_speed_10m', 'wind_speed_80m']
    for col in wind_columns:
        if col in df_hourly.columns and df_hourly[col].notnull().all():
            df_hourly[f'{col}_m_per_s'] = df_hourly[col].apply(knots_to_mps)
        else:
            df_hourly[f'{col}_m_per_s'] = None

    # Преобразование видимости
    if 'visibility' in df_hourly.columns and df_hourly['visibility'].notnull().all():
        df_hourly['visibility_m'] = df_hourly['visibility'].apply(ft_to_m)
    else:
        df_hourly['visibility_m'] = None

    # Агрегация за 24 часа
    agg_funcs = {
        'temperature_2m_celsius': 'mean',
        'relative_humidity_2m': 'mean',
        'dew_point_2m_celsius': 'mean',
        'apparent_temperature_celsius': 'mean',
        'temperature_80m_celsius': 'mean',
        'temperature_120m_celsius': 'mean',
        'wind_speed_10m_m_per_s': 'mean',
        'wind_speed_80m_m_per_s': 'mean',
        'visibility_m': 'mean',
        'rain_mm': 'sum',
        'showers_mm': 'sum',
        'snowfall_mm': 'sum'
    }
    stats_by_24h = df_hourly.groupby('date').agg(agg_funcs).round(5)

    # Переименование для итоговой таблицы
    stats_by_24h.rename(columns={
        'temperature_2m_celsius': 'avg_temperature_2m_24h',
        'relative_humidity_2m': 'avg_relative_humidity_2m_24h',
        'dew_point_2m_celsius': 'avg_dew_point_2m_24h',
        'apparent_temperature_celsius': 'avg_apparent_temperature_24h',
        'temperature_80m_celsius': 'avg_temperature_80m_24h',
        'temperature_120m_celsius': 'avg_temperature_120m_24h',
        'wind_speed_10m_m_per_s': 'avg_wind_speed_10m_24h',
        'wind_speed_80m_m_per_s': 'avg_wind_speed_80m_24h',
        'visibility_m': 'avg_visibility_24h',
        'rain_mm': 'total_rain_24h',
        'showers_mm': 'total_showers_24h',
        'snowfall_mm': 'total_snowfall_24h'
    }, inplace=True)

    # Объединение с daily
    df_merged = pd.merge(df_hourly, df_daily, on='date', how='left')

    # Фильтрация дневного времени
    df_daylight = df_merged[(df_merged['datetime'] >= df_merged['sunrise_dt']) & (df_merged['datetime'] <= df_merged['sunset_dt'])]

    # Агрегация по дневному времени
    stats_by_daylight = df_daylight.groupby('date').agg(agg_funcs).round(5)
    stats_by_daylight.rename(columns={
        'temperature_2m_celsius': 'avg_temperature_2m_daylight',
        'relative_humidity_2m': 'avg_relative_humidity_2m_daylight',
        'dew_point_2m_celsius': 'avg_dew_point_2m_daylight',
        'apparent_temperature_celsius': 'avg_apparent_temperature_daylight',
        'temperature_80m_celsius': 'avg_temperature_80m_daylight',
        'temperature_120m_celsius': 'avg_temperature_120m_daylight',
        'wind_speed_10m_m_per_s': 'avg_wind_speed_10m_daylight',
        'wind_speed_80m_m_per_s': 'avg_wind_speed_80m_daylight',
        'visibility_m': 'avg_visibility_daylight',
        'rain_mm': 'total_rain_daylight',
        'showers_mm': 'total_showers_daylight',
        'snowfall_mm': 'total_snowfall_daylight'
    }, inplace=True)

    # Список необходимых для итоговой таблицы полей
    columns_to_keep = [
        'date', 'datetime', 'wind_speed_10m_m_per_s', 'wind_speed_80m_m_per_s',
        'temperature_2m_celsius', 'apparent_temperature_celsius', 'temperature_80m_celsius',
        'temperature_120m_celsius', 'soil_temperature_0cm_celsius', 'soil_temperature_6cm_celsius',
        'rain_mm', 'showers_mm', 'snowfall_mm', 'daylight_hours', 'sunrise_iso', 'sunset_iso'
    ]
    df_merged_selected = df_merged[columns_to_keep]

    df_prefinal = pd.merge(df_merged_selected, stats_by_24h, on='date', how='left')
    df_final = pd.merge(df_prefinal, stats_by_daylight, on='date', how='left')

    return df_final

def save_csv(df):
    filename = "data/final_table.csv"
    df.to_csv(filename, sep=';', index=False)

if __name__ == '__main__':

    print("Загрузка данных с API Open-Meteo")
    json_data = fetch_data(API_URL)
    print("Преобразование данных")
    final_df = transform_data(json_data)
    print("Сохранение CSV")
    save_csv(final_df)
    print("Данные успешно сохранены в data/final_table.csv")

