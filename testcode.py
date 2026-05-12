
import pandas as pd
import numpy as np

from sklearn.preprocessing import LabelEncoder, StandardScaler

df = pd.read_csv(r"D:\BigData\BigData\DataSets\IBB\traffic_density_202501.csv")

print("İlk Boyut:", df.shape)
print(df.columns)
print(df.head())
df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"], errors="coerce")

# bozuk tarihleri sil
df = df.dropna(subset=["DATE_TIME"])

print("\nEksik Veriler:\n")
print(df.isnull().sum())

# sayısal kolonları median ile doldur
num_cols = [
    "LATITUDE",
    "LONGITUDE",
    "MINIMUM_SPEED",
    "MAXIMUM_SPEED",
    "AVERAGE_SPEED",
    "NUMBER_OF_VEHICLES"
]

for col in num_cols:
    df[col] = df[col].fillna(df[col].median())

# GEOHASH boşsa doldur
df["GEOHASH"] = df["GEOHASH"].fillna("unknown")
df["year"] = df["DATE_TIME"].dt.year
df["month"] = df["DATE_TIME"].dt.month
df["day"] = df["DATE_TIME"].dt.day
df["hour"] = df["DATE_TIME"].dt.hour
df["minute"] = df["DATE_TIME"].dt.minute
df["weekday"] = df["DATE_TIME"].dt.weekday
df["weekend"] = np.where(df["weekday"] >= 5, 1, 0)

df = df[df["MINIMUM_SPEED"] >= 0]
df = df[df["MAXIMUM_SPEED"] >= 0]
df = df[df["AVERAGE_SPEED"] >= 0]
df = df[df["NUMBER_OF_VEHICLES"] >= 0]

# minimum <= average <= maximum
df = df[df["MINIMUM_SPEED"] <= df["MAXIMUM_SPEED"]]

df = df[
    (df["AVERAGE_SPEED"] >= df["MINIMUM_SPEED"]) &
    (df["AVERAGE_SPEED"] <= df["MAXIMUM_SPEED"])
]

# aşırı hız temizliği
df = df[df["MAXIMUM_SPEED"] <= 180]
Q1 = df["NUMBER_OF_VEHICLES"].quantile(0.25)
Q3 = df["NUMBER_OF_VEHICLES"].quantile(0.75)
IQR = Q3 - Q1

alt = Q1 - 1.5 * IQR
ust = Q3 + 1.5 * IQR

df = df[
    (df["NUMBER_OF_VEHICLES"] >= alt) &
    (df["NUMBER_OF_VEHICLES"] <= ust)
]

df = df.sort_values(["GEOHASH", "DATE_TIME"]).reset_index(drop=True)

le = LabelEncoder()
df["GEOHASH_ENCODED"] = le.fit_transform(df["GEOHASH"])

# trafik yoğunluk skoru
df["traffic_density"] = df["NUMBER_OF_VEHICLES"] / (df["AVERAGE_SPEED"] + 1)

# hız aralığı
df["speed_range"] = df["MAXIMUM_SPEED"] - df["MINIMUM_SPEED"]

# rush hour
df["rush_hour"] = np.where(
    df["hour"].isin([7, 8, 9, 17, 18, 19]), 1, 0
)

# önceki hız
df["lag1_speed"] = df.groupby("GEOHASH")["AVERAGE_SPEED"].shift(1)

# önceki araç sayısı
df["lag1_vehicle"] = df.groupby("GEOHASH")["NUMBER_OF_VEHICLES"].shift(1)

# rolling ortalama hız
df["rolling3_speed"] = (
    df.groupby("GEOHASH")["AVERAGE_SPEED"]
    .rolling(3)
    .mean()
    .reset_index(level=0, drop=True)
)

# gelecekteki hedefler
df["future_speed"] = df.groupby("GEOHASH")["AVERAGE_SPEED"].shift(-1)
df["future_vehicle"] = df.groupby("GEOHASH")["NUMBER_OF_VEHICLES"].shift(-1)



# trafik seviyesi
df["traffic_level"] = pd.qcut(
    df["NUMBER_OF_VEHICLES"],
    q=3,
    labels=[0, 1, 2]
)

# tıkanıklık
df["congestion"] = np.where(
    (df["AVERAGE_SPEED"] < 25) &
    (df["NUMBER_OF_VEHICLES"] > df["NUMBER_OF_VEHICLES"].median()),
    1, 0
)

df = df.dropna()
scale_cols = [
    "MINIMUM_SPEED",
    "MAXIMUM_SPEED",
    "AVERAGE_SPEED",
    "NUMBER_OF_VEHICLES",
    "traffic_density",
    "speed_range",
    "lag1_speed",
    "lag1_vehicle",
    "rolling3_speed"
]

scaler = StandardScaler()
df[scale_cols] = scaler.fit_transform(df[scale_cols])

print("\nSon Boyut:", df.shape)
print(df.head())
print(df.info())

