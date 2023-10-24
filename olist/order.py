import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    """
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    """

    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        # pass  # YOUR CODE HERE
        orders = self.data["orders"].copy()
        orders = orders.loc[orders["order_status"] == "delivered"].copy()

        orders["order_delivered_customer_date"] = pd.to_datetime(
            orders["order_delivered_customer_date"]
        )
        orders["order_purchase_timestamp"] = pd.to_datetime(
            orders["order_purchase_timestamp"]
        )
        orders["order_estimated_delivery_date"] = pd.to_datetime(
            orders["order_estimated_delivery_date"]
        )

        orders["wait_time"] = (
            orders["order_delivered_customer_date"] - orders["order_purchase_timestamp"]
        ).dt.days

        orders["expected_wait_time"] = (
            orders["order_estimated_delivery_date"] - orders["order_purchase_timestamp"]
        ).dt.days

        orders["delay_vs_expected"] = (
            (
                orders["order_delivered_customer_date"]
                - orders["order_estimated_delivery_date"]
            ).dt.days
        ).apply(lambda x: x if x > 0 else 0)

        custom_orders = orders[
            [
                "order_id",
                "wait_time",
                "expected_wait_time",
                "delay_vs_expected",
                "order_status",
            ]
        ].copy()

        return custom_orders

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        custom_reviews = self.data["order_reviews"].copy()
        custom_reviews["dim_is_five_star"] = (
            custom_reviews["review_score"] == 5
        ).astype(int)
        custom_reviews["dim_is_one_star"] = (
            custom_reviews["review_score"] == 1
        ).astype(int)

        reviews_df = custom_reviews[
            ["order_id", "dim_is_five_star", "dim_is_one_star", "review_score"]
        ].copy()
        return reviews_df

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """

        orders = self.data["order_items"].copy()
        custom_orders = orders[["order_id", "order_item_id"]].copy()
        custom_orders["number_of_products"] = custom_orders.groupby("order_id")[
            "order_item_id"
        ].transform("count")
        custom_orders = custom_orders.drop_duplicates(subset=["order_id"])
        return custom_orders[["order_id", "number_of_products"]]

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        orders = self.data["order_items"].copy()
        custom_orders = orders[["order_id", "seller_id"]].copy()
        custom_orders["number_of_sellers"] = custom_orders.groupby("order_id")[
            "seller_id"
        ].transform("count")
        custom_orders = custom_orders.drop_duplicates(subset=["order_id"])
        return custom_orders[["order_id", "number_of_sellers"]]

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        orders = self.data["order_items"].copy()
        custom_orders = orders[["order_id", "price", "freight_value"]].copy()
        custom_orders = (
            custom_orders.groupby("order_id")
            .agg({"price": "sum", "freight_value": "sum"})
            .reset_index()
        )

        return custom_orders

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        orders = self.data["orders"].copy()
        order_items = self.data["order_items"].copy()
        sellers = self.data["sellers"].copy()
        customers = self.data["customers"].copy()
        geo_locations = (
            self.data["geolocation"]
            .groupby("geolocation_zip_code_prefix", as_index=False)
            .first()
            .copy()
        )

        # Get geolocation_lat, geolocation_lng from sellers and customers
        sellers_geo = sellers.merge(
            geo_locations,
            left_on="seller_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
        )
        customers_geo = customers.merge(
            geo_locations,
            left_on="customer_zip_code_prefix",
            right_on="geolocation_zip_code_prefix",
        )

        # Merge all dataframes
        customers_sellers = (
            customers.merge(orders, on="customer_id")
            .merge(order_items, on="order_id")
            .merge(sellers, on="seller_id")
            .merge(sellers_geo, on="seller_id")
            .merge(customers_geo, on="customer_id", suffixes=("_seller", "_customer"))
            .dropna()
        )

        # Calculate distance between seller and customer
        customers_sellers["distance_seller_customer"] = customers_sellers.apply(
            lambda x: haversine_distance(
                x["geolocation_lng_seller"],
                x["geolocation_lat_seller"],
                x["geolocation_lng_customer"],
                x["geolocation_lat_customer"],
            ),
            axis=1,
        )

        df = customers_sellers.groupby("order_id", as_index=False).agg(
            {"distance_seller_customer": "mean"}
        )
        # Select relevant columns and return dataframe

        df = df[["order_id", "distance_seller_customer"]].copy()
        return df

    def get_training_data(self, is_delivered=True, with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above

        # Merge all dataframes
        custom_orders = (
            self.get_wait_time(is_delivered)
            .merge(self.get_review_score(), on="order_id")
            .merge(self.get_number_products(), on="order_id")
            .merge(self.get_number_sellers(), on="order_id")
            .merge(self.get_price_and_freight(), on="order_id")
        )

        if with_distance_seller_customer:
            custom_orders = custom_orders.merge(
                self.get_distance_seller_customer(), on='order_id')

        return custom_orders.dropna()
