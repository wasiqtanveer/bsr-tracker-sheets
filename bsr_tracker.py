import os
import json
import keepa
from datetime import datetime
import gspread
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

# --- CONFIG ---
KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY")
SHEET_ID = os.environ.get("SHEET_ID", "1B_ZGYEJADFpB90kVX5Ix6LudOgYleca6OJOFZs70sGU")
DATA_FILE = "bsr_data.json"

CLIENTS = [
    {
        "name": "Client 1",
        "domain": "FR",
        "asins": ["B0CQ6S5MMY", "B0D7Y1363J"],
    },
    {
        "name": "Client 1 (UK)",
        "domain": "GB",  # GB is used for Amazon UK
        "asins": ["B001PHBZFQ"],
    },
    {
        "name": "Client 1 (US)",
        "domain": "US",
        "asins": ["B00FXNAAW2"],
    }
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def get_sheets_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if creds_json:
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
    return gspread.authorize(creds)

def load_previous_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_current_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_trend_arrow(today, yesterday):
    if yesterday is None:
        return "🆕 new"
    if today < yesterday:
        return "improved ↑"
    elif today > yesterday:
        return "worsened ↓"
    else:
        return "unchanged →"

def fetch_bsr(asin, domain):
    api = keepa.Keepa(KEEPA_API_KEY)
    products = api.query([asin], stats=1, history=True, rating=True, buybox=True, domain=domain)

    print("Products returned:", len(products) if products else 0)
    if products:
        print("Product keys:", list(products[0].keys())[:10])

    if not products:
        return None

    product = products[0]
    title = product.get("title", "Unknown Product")
    image_url = None
    
    # Check for 'images' array or string 'imagesCSV'
    images_array = product.get("images", [])
    if images_array and isinstance(images_array, list):
        # Format is usually [{'l': 'filename.jpg', ...}] or just strings
        first_img = images_array[0]
        if isinstance(first_img, dict) and 'l' in first_img:
            file_name = first_img['l']
        elif isinstance(first_img, str):
            file_name = first_img
        else:
            file_name = None
            
        if file_name:
            if not file_name.startswith("http"):
                image_url = f"https://images-na.ssl-images-amazon.com/images/I/{file_name}"
            else:
                image_url = file_name
    elif product.get("imagesCSV"):
        first_image = str(product.get("imagesCSV")).split(",")[0]
        if not first_image.startswith("http"):
            image_url = f"https://images-na.ssl-images-amazon.com/images/I/{first_image}"
        else:
            image_url = first_image

    current_rating = None
    current_reviews = None
    buybox_status = "N/A"

    stats = product.get("stats", {})
    current_data = stats.get("current", [])

    if len(current_data) > 16 and current_data[16] is not None and current_data[16] > 0:
        current_rating = current_data[16] / 10
    if len(current_data) > 17 and current_data[17] is not None and current_data[17] > 0:
        current_reviews = current_data[17]

    buybox_sellers = product.get("buyBoxSellerIdHistory", [])
    if buybox_sellers:
        latest_seller = buybox_sellers[-1] if buybox_sellers else None
        if latest_seller and latest_seller != "ATVPDKIKX0DER":
            buybox_status = "⚠️ Piggybacker detected"
        else:
            buybox_status = "✅ No Piggybacker"

    sales_ranks = product.get("salesRanks", {})
    category_tree = product.get("categoryTree", [])

    main_category_id = None
    main_category_name = None
    main_bsr = None
    sub_category_id = None
    sub_category_name = None
    sub_bsr = None

    if sales_ranks:
        rank_items = list(sales_ranks.items())
        if len(rank_items) >= 1:
            main_category_id = rank_items[0][0]
            ranks = rank_items[0][1]
            if ranks and len(ranks) >= 2:
                main_bsr = ranks[-1]
        if len(rank_items) >= 2:
            sub_category_id = rank_items[1][0]
            ranks = rank_items[1][1]
            if ranks and len(ranks) >= 2:
                sub_bsr = ranks[-1]

    for cat in category_tree:
        cat_id = str(cat.get("catId", ""))
        cat_name = cat.get("name", "Unknown")
        if cat_id == str(main_category_id):
            main_category_name = cat_name
        if cat_id == str(sub_category_id):
            sub_category_name = cat_name

    if not main_category_name:
        main_category_name = f"Category {main_category_id}"
    if not sub_category_name and sub_category_id:
        sub_category_name = f"Subcategory {sub_category_id}"

    return {
        "title": title,
        "image_url": image_url,
        "main_bsr": main_bsr,
        "main_category": main_category_name,
        "sub_bsr": sub_bsr,
        "sub_category": sub_category_name,
        "rating": current_rating,
        "reviews": current_reviews,
        "buybox": buybox_status,
    }

def update_sheet(gc, asin, domain, data):
    today = datetime.now().strftime("%b %d, %Y")

    try:
        sh = gc.open_by_key(SHEET_ID)
    except Exception as e:
        print(f"❌ Could not open sheet: {e}")
        return

    sheet_name = f"{domain} MARKET PLACE"
    try:
        ws = sh.worksheet(sheet_name)
    except:
        ws = sh.add_worksheet(title=sheet_name, rows=50, cols=50)

    # Initialize standard header text
    ws.update_cell(1, 1, "DAILY MONITORING")
    ws.update_cell(2, 1, f"{domain} MARKET PLACE")

    # Apply styles block
    try:
        # Style "DAILY MONITORING" 
        ws.format("A1:Z1", {
            "backgroundColorStyle": {"rgbColor": {"red": 0.8, "green": 0.1, "blue": 0.1}},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}, "fontSize": 12},
            "horizontalAlignment": "CENTER"
        })
        # Style "{domain} MARKET PLACE"
        ws.format("A2:Z2", {
            "backgroundColorStyle": {"rgbColor": {"red": 0.95, "green": 0.8, "blue": 0.8}},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}, "fontSize": 11},
            "horizontalAlignment": "CENTER"
        })
        # Style Dates Row
        ws.format("A3:Z3", {
            "backgroundColorStyle": {"rgbColor": {"red": 1.0, "green": 0.9, "blue": 0.9}},
            "textFormat": {"bold": True, "foregroundColorStyle": {"rgbColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}},
            "horizontalAlignment": "CENTER"
        })
    except Exception as e:
        print(f"⚠️ Could not apply header formatting: {e}")

    # Ensure main title spans visually
    try:
        ws.merge_cells("A1:C1", merge_type="MERGE_ALL")
        ws.merge_cells("A2:C2", merge_type="MERGE_ALL")
    except Exception:
        pass  # Already merged or overlaps

    all_values = ws.get_all_values()

    # Dates are on row 3 in the preferred layout
    date_row_idx = 2  # 0-indexed in all_values structure
    if len(all_values) <= date_row_idx:
        header_row = []
    else:
        header_row = all_values[date_row_idx]

    date_col = None
    for j, cell in enumerate(header_row):
        if cell == today:
            date_col = j + 1
            break

    if date_col is None:
        # Instead of appending to the right, insert a new column at 3 (Column C)
        # This pushes all previous dates to the right.
        date_col = 3
        # Empty values for Row 1 and 2, and the Date for Row 3.
        ws.insert_cols([["", "", today]], col=3)
        # Refresh all_values since we just altered the sheet columns
        all_values = ws.get_all_values()

    asin_row = None
    for i, row in enumerate(all_values):
        if row and row[0] == asin:
            asin_row = i + 1
            break

    if asin_row is None:
        # Start placing ASINs at row 4 minimum, leaving an empty row between products
        max_filled_row = 3
        for i, row in enumerate(all_values):
            if row and any(cell.strip() for cell in row):
                max_filled_row = max(max_filled_row, i + 1)
        
        if max_filled_row >= 4:
            next_row = max_filled_row + 2
        else:
            next_row = 4

        asin_row = next_row
        ws.update_cell(asin_row, 1, asin)
        
        # Merge cells for the image to span 5 rows in column A
        try:
            ws.merge_cells(f"A{asin_row + 1}:A{asin_row + 5}", merge_type="MERGE_ALL")
        except Exception:
            pass
        
        # Labels go in column B (column 2)
        ws.update_cell(asin_row + 1, 2, "BUYBOX")
        ws.update_cell(asin_row + 2, 2, "REVIEWS")
        ws.update_cell(asin_row + 3, 2, "RATING")
        ws.update_cell(asin_row + 4, 2, "BSR (Main)")
        ws.update_cell(asin_row + 5, 2, "BSR (Sub)")

        # Style the ASIN title row
        try:
            ws.format(f"A{asin_row}:Z{asin_row}", {
                "backgroundColorStyle": {"rgbColor": {"red": 0.9, "green": 0.9, "blue": 0.9}},
                "textFormat": {"bold": True, "fontSize": 11}
            })
            # Style the labels
            ws.format(f"B{asin_row + 1}:B{asin_row + 5}", {
                "backgroundColorStyle": {"rgbColor": {"red": 0.95, "green": 0.95, "blue": 0.95}},
                "textFormat": {"bold": True},
                "horizontalAlignment": "LEFT"
            })
            # Add some borders (optional but looks good)
            ws.format(f"A{asin_row}:Z{asin_row + 5}", {
                "borders": {
                    "top": {"style": "SOLID", "colorStyle": {"rgbColor": {"red": 0.8, "green": 0.8, "blue": 0.8}}},
                    "bottom": {"style": "SOLID", "colorStyle": {"rgbColor": {"red": 0.8, "green": 0.8, "blue": 0.8}}}
                }
            })
        except Exception as e:
            print(f"⚠️ Could not apply row formatting: {e}")

    rating_str = f"{data['rating']:.1f} star rating" if data['rating'] else "N/A"
    reviews_str = f"{int(data['reviews']):,} ratings" if data['reviews'] else "N/A"
    main_bsr_str = f"#{data['main_bsr']:,} in {data['main_category']}" if data['main_bsr'] else "N/A"
    sub_bsr_str = f"#{data['sub_bsr']:,} in {data['sub_category']}" if data['sub_bsr'] else "N/A"

    # Always ensure the image is inserted in the merged A column
    if data.get('image_url'):
        ws.update_cell(asin_row + 1, 1, f'=IMAGE("{data["image_url"]}")')

    updates = [
        {"range": f"{rowcol_to_a1(asin_row + 1, date_col)}", "values": [[data['buybox']]]},
        {"range": f"{rowcol_to_a1(asin_row + 2, date_col)}", "values": [[reviews_str]]},
        {"range": f"{rowcol_to_a1(asin_row + 3, date_col)}", "values": [[rating_str]]},
        {"range": f"{rowcol_to_a1(asin_row + 4, date_col)}", "values": [[main_bsr_str]]},
        {"range": f"{rowcol_to_a1(asin_row + 5, date_col)}", "values": [[sub_bsr_str]]},
    ]
    ws.batch_update(updates)

    print(f"✅ Sheet updated for {asin}")

def main():
    print(f"🚀 BSR Tracker running — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    previous_data = load_previous_data()
    current_data = {}

    gc = get_sheets_client()

    for client in CLIENTS:
        domain = client["domain"]

        for asin in client["asins"]:
            print(f"📦 Fetching data for ASIN: {asin} ({domain})")
            try:
                data = fetch_bsr(asin, domain)
                if data:
                    current_data[asin] = data
                    update_sheet(gc, asin, domain, data)
                else:
                    print(f"⚠️ No data returned for {asin}")
            except Exception as e:
                print(f"❌ Error fetching {asin}: {e}")

    save_current_data(current_data)
    print("✅ Done. Data saved.")

if __name__ == "__main__":
    main()