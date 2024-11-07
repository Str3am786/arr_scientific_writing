import logging
from time import sleep

import requests
import pandas as pd

API_KEY = ""
BASE_URL = "https://maps.googleapis.com/maps/api/place/"


def search_hiking_places(location, radius, api_key=API_KEY):
    url = f"{BASE_URL}textsearch/json"
    params = {
        'query': 'hiking trail',
        'location': location,  # e.g., '40.7128,-74.0060' for New York City
        'radius': radius,      # in meters, e.g., 50000 for 50 km
        'type': 'park',
        'key': api_key
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    results = response.json().get('results', [])
    return [place.get("place_id") for place in results]


def get_place_id(place_name: str, api_key=API_KEY):
    """
    Get the Place ID for a given place name.
    """
    url = f"{BASE_URL}findplacefromtext/json"
    params = {
        'input': place_name,
        'inputtype': 'textquery',
        'fields': 'place_id',
        'key': api_key
    }
    response = requests.get(url, params=params)

    response.raise_for_status()

    candidates = response.json().get('candidates')

    if candidates:
        return candidates[0].get('place_id')
    else:
        raise Exception("Place not found.")


def get_place_reviews(place_id: str, api_key=API_KEY):
    """
    Get reviews for a place using its Place ID.
    """
    url = f"{BASE_URL}details/json"
    params = {
        'place_id': place_id,
        'fields': 'name,rating,reviews',
        'key': api_key
    }
    response = requests.get(url, params=params)
    # If NOT 200
    response.raise_for_status()
    # else

    result = response.json()
    if result.get('result'):
        return result['result']
    else:
        raise Exception(f"No reviews for place {place_id}")


def clean_up_reviews(dict_new_reviews: dict, filtered_reviews: pd.DataFrame) -> pd.DataFrame:
    """
    :param list_new_reviews:
    :param filtered_reviews: Already cleaned-up reviews from same place
    :return:
    List of reviews with what we want to know/need to know
    """
    list_new_reviews = dict_new_reviews.get("reviews", [])
    new_reviews = [
        {
            "name": dict_new_reviews.get("name",""),
            "rating": review.get("rating"),
            "text": review.get("text"),
            "positive": True if int(review.get("rating", 0)) > 4 else False,
        }
        for review in list_new_reviews
        if review.get("language") == "en" and review.get("original_language") == "en"
    ]

    new_reviews_df = pd.DataFrame(new_reviews)

    # Concatenate the new reviews with the existing filtered_reviews DataFrame
    return new_reviews_df



def get_review_pipeline(location, radius) -> pd.DataFrame:
    """
    Orchestrates the process to get and clean reviews for a given place name.
    """
    result = pd.DataFrame()

    try:
        hiking_places = search_hiking_places(location=location, radius=radius)
    except Exception as e:
        logging.error(f"{e}")
        return result

    for place_id in hiking_places:
        sleep(0.5)
        try:
            unclean_reviews = get_place_reviews(place_id=place_id)
            filtered_reviews = clean_up_reviews(unclean_reviews, result)
            result = pd.concat([filtered_reviews, result], ignore_index=True)
        except Exception as e:
            logging.error(f"{e}")
            return result

    return result
