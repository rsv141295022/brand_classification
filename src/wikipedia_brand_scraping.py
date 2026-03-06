import re, requests, time
from bs4 import BeautifulSoup
import pandas as pd

def scrape_wikipedia_between_sections(parent_name, url, start_section_text, end_section_text, parent_identifiers):

    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
    # Bypass proxy that may block Wikipedia
    response = requests.get(url, headers=headers, proxies={"http": None, "https": None})
    soup = BeautifulSoup(response.text, 'html.parser')
    
    content = soup.find('div', class_='mw-parser-output')
    if not content:
        print(f"Warning: Could not find content for {parent_name}")
        return pd.DataFrame()

    # Find the boundary headers
    start_node = content.find(lambda tag: tag.name in ['h2', 'h3'] and start_section_text.lower() in tag.get_text().lower())
    end_node = content.find(lambda tag: tag.name in ['h2', 'h3'] and end_section_text.lower() in tag.get_text().lower())
    
    # Forbidden keywords to exclude inactive or non-owned brands
    forbidden_keywords = [
        "sold", "acquired", "divested", "spun off", "phased out", 
        "discontinued", "merged", "licensed", "taken over", "former"
    ]

    brand_lists = []

    if start_node:
        # Search all elements following the start header
        for element in start_node.find_all_next():
            
            # Stop if we hit the end header
            if element == end_node:
                break
            
            # Process lists
            if element.name == 'ul':
                items = element.find_all('li')
                for li in items:
                    # 1. Clean the brand name (remove citations [1] and secondary descriptions)
                    brand_raw = re.sub(r'\[.*?\]', '', li.get_text()).split(':')[0].split('–')[0].split('-')[0].strip()
                    brand_lower = brand_raw.lower()
                    
                    # 2. Skip if brand is too short or empty
                    if len(brand_raw) <= 2: 
                        continue

                    # 3. Apply forbidden keywords filter
                    # If any forbidden word is in the full line text, skip it
                    full_line_text = li.get_text().lower()
                    if any(word in full_line_text for word in forbidden_keywords):
                        continue

                    # 4. Classify Parent vs Child
                    is_parent = any(id_name.lower() in brand_lower for id_name in parent_identifiers)
                    
                    brand_lists.append({
                        'group_name': brand_raw,
                        'parent_company': parent_name,
                        'organization_type': 'parent_brand' if is_parent else 'child_brand'
                    })
    else:
        print(f"Warning: Start section '{start_section_text}' not found for {parent_name}")

    # Always add parent company as a row (some Wikipedia lists don't include it)
    has_parent = any(b['group_name'] == parent_name for b in brand_lists)
    if not has_parent:
        brand_lists.insert(0, {
            'group_name': parent_name,
            'parent_company': parent_name,
            'organization_type': 'parent_brand'
        })

    df = pd.DataFrame(brand_lists).drop_duplicates(subset=['group_name'])
    return df


if __name__ == "__main__":
    companies = {
        "Procter & Gamble": (
            "https://en.wikipedia.org/wiki/List_of_Procter_%26_Gamble_brands", 
            "Brands with net sales of more than US$1 billion annually",
            "Discontinued brands",
            ["procter & gamble", "p&g"]
            ),
        "PepsiCo": (
            "https://en.wikipedia.org/wiki/List_of_assets_owned_by_PepsiCo",
            "Trademarks",
            "Largest brands by sales",
            ["pepsico", "pepsi"]
            ),
        "Nestlé": (
            "https://en.wikipedia.org/wiki/List_of_Nestl%C3%A9_brands",
            "Beverages",
            "References",
            ["nestlé", "nestle"]
            ),
        "Colgate-Palmolive": (
            "https://en.wikipedia.org/wiki/Colgate-Palmolive#Brands",
            "Brands",
            "Discontinued products and former brands",
            ["colgate-palmolive"]
            ),
        "L'Oréal": (
            "https://en.wikipedia.org/wiki/L%27Or%C3%A9al#Brands",
            "Brand portfolio",
            "Marketing", 
            ["l'oréal", "l'oreal"]
            ),
        "Coca-Cola Company": (
            "https://en.wikipedia.org/wiki/List_of_Coca-Cola_brands",
            "A",
            "References",
            ["coca-cola company", "coca-cola"]
            ),
        # "Johnson & Johnson": ("https://en.wikipedia.org/wiki/Johnson_%26_Johnson#Consumer_health", ["johnson & johnson", "j&j"]),
        "Kraft Heinz": (
            "https://en.wikipedia.org/wiki/Kraft_Heinz#Brands",
            "Brands",
            "Finance",
            ["kraft heinz", "kraft", "heinz"]
            ),
        "Mondelez International": (
            "https://en.wikipedia.org/wiki/List_of_Mondelez_International_brands",
            "Current brands",
            "Former brands",
            ["mondelez international", "mondelez"]),
        "Unilever": (
            "https://en.wikipedia.org/wiki/List_of_Unilever_brands",
            "Food and drink",
            "Former brands",
            ["unilever"]
            )
    }

    master_frames = []

    for name, (url, start, end, ids) in companies.items():
        df_temp = scrape_wikipedia_between_sections(
            url=url, parent_name=name, start_section_text=start,
            end_section_text=end, parent_identifiers=ids
        )
        n = len(df_temp)
        print(f"Processing: {name}... {n} brands")
        if not df_temp.empty:
            master_frames.append(df_temp)
        time.sleep(1)  # Be kind to Wikipedia's servers

    final_df = pd.concat(master_frames, ignore_index=True).drop_duplicates(subset=['group_name', 'parent_company'])

    final_df.to_csv('./datasets/1_raw_data/wikipedia_brands.csv', index=False)