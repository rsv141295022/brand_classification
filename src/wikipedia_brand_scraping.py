import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_wikipedia_brand_list(url, parent_name, parent_identifiers):
    forbidden_keywords = [
        "sold", "acquired", "divested", "spun off", "phased out", 
        "discontinued", "merged", "licensed", "taken over", "former"
    ]

    # Send a GET request to the URL
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')

    # The brands are located in the 'mw-parser-output' div
    # content = soup.find('div', class_='mw-parser-output')

    content = soup.find('div', class_='mw-content-ltr mw-parser-output')

    # We find all <ul> tags that follow section headers
    # These typically contain the brand names
    brand_markets = content.find_all('ul')

    brand_lists = []

    for ul in brand_markets:
        items = ul.find_all('li')
        for li in items:
            brand_raw = li.get_text().split('[')[0].strip()
            brand_lower = brand_raw.lower() 
            
            # Skip if it contains forbidden words
            is_invalid = any(word in brand_lower for word in forbidden_keywords)
            if is_invalid or len(brand_raw) <= 2:
                continue

            # 2. Logic to detect Parent vs Child
            # We check if the string IS the parent name or JUST the parent name
            is_parent = any(id_name == brand_lower for id_name in parent_identifiers)
            
            # If the string contains "Nestlé" but has other words (e.g. "Nestlé Pure Life")
            # it is usually still a child brand, but if it's just "Nestlé", it's the parent.
            if is_parent:
                org_type = 'parent_brand'
            else:
                org_type = 'child_brand'
                
            brand_lists.append({
                'group_name': brand_raw,
                'parent_company': parent_name,
                'organization_type': org_type
            })

    df = pd.DataFrame(brand_lists)
    df = df.drop_duplicates(subset=['group_name'])
    
    return df

if __name__ == "__main__":
    companies = {
        "Procter & Gamble": ("https://en.wikipedia.org/wiki/List_of_Procter_%26_Gamble_brands", ["procter & gamble", "p&g"]),
        "PepsiCo": ("https://en.wikipedia.org/wiki/List_of_assets_owned_by_PepsiCo", ["pepsico", "pepsi"]),
        "Nestlé": ("https://en.wikipedia.org/wiki/List_of_Nestl%C3%A9_brands", ["nestlé", "nestle"]),
        "Colgate-Palmolive": ("https://en.wikipedia.org/wiki/Colgate-Palmolive#Brands", ["colgate-palmolive"]),
        "L'Oréal": ("https://en.wikipedia.org/wiki/L%27Or%C3%A9al#Brands", ["l'oréal", "l'oreal"]),
        "Coca-Cola Company": ("https://en.wikipedia.org/wiki/List_of_Coca-Cola_brands", ["coca-cola company", "coca-cola"]),
        "Johnson & Johnson": ("https://en.wikipedia.org/wiki/Johnson_%26_Johnson#Consumer_health", ["johnson & johnson", "j&j"]),
        "Kraft Heinz": ("https://en.wikipedia.org/wiki/Kraft_Heinz#Brands", ["kraft heinz", "kraft", "heinz"]),
        "Mondelez International": ("https://en.wikipedia.org/wiki/Mondelez_International#Brands", ["mondelez international", "mondelez"]),
        "Unilever": ("https://en.wikipedia.org/wiki/List_of_Unilever_brands", ["unilever"])
    }

    master_frames = []

    for name, (url, ids) in companies.items():
        print(f"Processing: {name}...")
        df_temp = scrape_wikipedia_brand_list(url, name, ids)
        if not df_temp.empty:
            master_frames.append(df_temp)
        time.sleep(1) # Be kind to Wikipedia's servers

    final_df = pd.concat(master_frames, ignore_index=True).drop_duplicates(subset=['group_name', 'parent_company'])

    final_df.to_csv('./datasets/1_raw_data/wikipedia_brands.csv', index=False)