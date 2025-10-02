"""
Main pipeline for the project procar
"""

import os
from loguru import logger

from src.preprocessing import load_data, split_data
from src.train import train_model, evaluate_model
from src.inference import save_model


def main() -> None:
    """
    Main pipeline for the project procar
    """
    logger.info("-----------------")
    logger.info("Starting pipeline")
    logger.info("Loading data")
    df = load_data()

    logger.info("Splitting data")
    train, test = split_data(df)

    logger.info("Training model")
    model = train_model(train)

    logger.info("Saving model")
    os.makedirs("models", exist_ok=True)
    model_path = os.path.join("models", "car_price_model.joblib")
    save_model(model, model_path)

    logger.info("Evaluating model")
    metrics = evaluate_model(model, test)
    logger.info(f"Model evaluation: {metrics}")

    logger.info("Pipeline complete")


if __name__ == "__main__":
    main()
