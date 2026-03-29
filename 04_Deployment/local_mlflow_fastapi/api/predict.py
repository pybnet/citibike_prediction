
import pandas as pd
import holidays
from datetime import date

def predict_from_user_date(dataset, request_datetime, model, weather_df, update_columns,
                           station_id=None):
    df = dataset.copy()
    request_datetime = pd.to_datetime(request_datetime)

    # Extract temporal features from request_datetime
    month_req = request_datetime.month
    dow_req = request_datetime.dayofweek

    # Filter historical data
    df_filtered = df[
        (df["month"] == month_req) &
        (df["day"].apply(lambda d: pd.Timestamp(year=df["year"].iloc[0], month=month_req, day=d).dayofweek) == dow_req)
    ]

    # filtrer station si nécessaire
    if station_id is not None:
        df_filtered = df_filtered[df_filtered["station_id"] == station_id]

    # trier
    df_filtered = df_filtered.sort_values(["station_id", "hour"])
 
    # Take the last observation (latest hour in filtered data)
    if df_filtered.empty:
        # If no matching rows, create an empty DataFrame with feature columns
        X = pd.DataFrame(columns=model.feature_names_in_)
    else:
        X = df_filtered[model.feature_names_in_].iloc[-1:]

    # Get US holidays
    us_holidays = holidays.US(years=request_datetime.year)
    
    # Données météo actuelles
    for col in update_columns:
        if col in X.columns and col in weather_df.columns:
            X[col] = weather_df[col].values[0]
        if col == "is_holiday":
            X[col] = 1 if request_datetime.date() in us_holidays else 0
    
    # prédiction
    prediction = round(float(model.predict(X)[0]), 1)
           
    Y_Neon_DB = X.copy()
    Y_Neon_DB["jour_semaine"] = dow_req
    
    print(f"Station {station_id} - Prévision net_flow prochaine heure on {request_datetime} : {prediction:.1f}")
    return prediction, Y_Neon_DB