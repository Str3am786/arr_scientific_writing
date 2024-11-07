from get_reviews import *

# Australia, North America continent, UK+ireland  Poland,
LOCATIONS = ["-24.423856010833198, 133.38725072590327",
             "42.577462956388764, -102.72249908756335",
             "53.84157080391751, -2.0579513789325485"
             "52.32402378949189, 18.708083464474228",
              ]
RADIUS = ["2200000", "4000000", "600000", "430000"]

CSV_FILE_PATH = "reviews.csv"

if __name__ == '__main__':
    final_review = pd.DataFrame()

    # If you want to start fresh each time, uncomment the following line:
    # pd.DataFrame().to_csv(csv_file_path, index=False)

    for i in range(len(LOCATIONS)):
        location = LOCATIONS[i]
        radius = RADIUS[i]

        try:
            reviews = get_review_pipeline(location, radius)

            # Check if reviews DataFrame is not empty
            if not reviews.empty:
                # Concatenate to final_review DataFrame (optional for in-memory work)
                final_review = pd.concat([final_review, reviews], ignore_index=True)

                # Append to CSV file
                if i == 0:  # Write header only for the first iteration
                    reviews.to_csv(CSV_FILE_PATH, mode='a', index=False)
                else:
                    reviews.to_csv(CSV_FILE_PATH, mode='a', header=False, index=False)

        except Exception as e:
            logging.error(f"Error with location {location} and radius {radius}: {e}")
            continue

    # At the end, you could save final_review to CSV if you want a final backup of the entire data
    final_review.to_csv("./final_review_backup.csv", index=False)



