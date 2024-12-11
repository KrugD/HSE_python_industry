import streamlit as st
import pandas as pd
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt

st.title("Анализ температурных данных и мониторинг текущей температуры через OpenWeatherMap API")
st.write("Это интерактивное приложение для мониторинга текущей температуры.")

# Функция для получения текущей температуры


async def get_current_temperature_async(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['main']['temp']
    else:
        st.error(f"Ошибка API: {response.json()['message']}")
        return None

# Функция для проверки нормальности температуры


def is_temperature_normal(temperature, df, city, season):
    season_stat = df.groupby(['city', 'season'])[
        'temperature'].agg(['mean', 'std']).reset_index()
    if city in season_stat.city.unique() and season in season_stat.season.unique():
        mean = season_stat[(season_stat['city'] == city) & (
            season_stat['season'] == season)]['mean'].iloc[0]
        std = season_stat[(season_stat['city'] == city) & (
            season_stat['season'] == season)]['std'].iloc[0]
        lower_bound = mean - 3 * std
        upper_bound = mean + 3 * std
        return lower_bound <= temperature <= upper_bound
    else:
        st.write(f"No historical data for {city} in {season}.")
        return None

# Функция для обработки исторических данных


def stat_city(city_data):
    seasonal_stats = city_data.groupby('season')['temperature'].agg([
        'mean', 'std']).reset_index()
    city_data = city_data.merge(
        seasonal_stats, on='season', suffixes=('', '_stats'))
    anomalies = city_data[(city_data['temperature'] < (city_data['mean'] - 2 * city_data['std'])) |
                          (city_data['temperature'] > (city_data['mean'] + 2 * city_data['std']))]
    return seasonal_stats, anomalies

# Получаем уникальные города


def anomals_func(df):
    cities = df['city'].unique()
    city_data_list = [df[df['city'] == city] for city in cities]

    # Используем ThreadPoolExecutor для многопоточности
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(stat_city, city_data_list))

    # Объединяем результаты
    season_stats_parallel = pd.concat([result[0] for result in results])
    anomalies_parallel = pd.concat([result[1] for result in results])
    return season_stats_parallel, anomalies_parallel


# Загрузка файла
uploaded_file = st.file_uploader(
    "Загрузите файл с историческими данными (temperature_data.csv)", type=["csv"])
if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Выбор города
    cities = df['city'].unique()
    selected_city = st.selectbox("Выберите город", cities)

    # Ввод API-ключа
    api_key = st.text_input("Введите ваш API-ключ OpenWeatherMap")

    # Отображение описательной статистики
    city_data = df[df['city'] == selected_city]
    st.write(city_data.describe())

    # Обработка данных для визуализации
    seasonal_stats, anomalies = anomals_func(df)

    # Визуализация временного ряда температур с аномалиями
    st.subheader("Временной ряд температур")
    plt.figure(figsize=(10, 5))
    plt.plot(city_data['timestamp'], city_data['temperature'],
             label='Температура', color='blue')

    # Выделяем аномалии
    if not anomalies.empty:
        plt.scatter(
            anomalies['timestamp'], anomalies['temperature'], color='red', label='Аномалии')

    plt.xlabel('Дата')
    plt.ylabel('Температура (°C)')
    plt.title(f'Температура в {selected_city}')
    plt.legend()
    st.pyplot(plt)

    # Сезонные профили
    st.subheader("Сезонные профили температур")
    st.line_chart(seasonal_stats.set_index('season')[['mean', 'std']])

    # Получение текущей температуры
    if api_key:
        current_temp = asyncio.run(
            get_current_temperature_async(selected_city, api_key))
        if current_temp is not None:
            # Определяем текущий сезон
            current_month = pd.to_datetime("now").month
            season = 'winter' if current_month in [12, 1, 2] else 'spring' if current_month in [
                3, 4, 5] else 'summer' if current_month in [6, 7, 8] else 'fall'

            if is_temperature_normal(current_temp, df, selected_city, season):
                st.success(
                    f"Температура в {selected_city}: {current_temp}°C (нормальная)")
            else:
                st.warning(
                    f"Температура в {selected_city}: {current_temp}°C (не нормальная)")
