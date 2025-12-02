import sys
import json
import os
import fitz  # PyMuPDF
import requests
import pandas as pd
from fuzzywuzzy import fuzz

# CONFIG

def upload_image_api(image_bytes, filename):
    url = "https://backend.succeedquiz.com/api/v1/upload"
    
    # 1. AUTHENTICATION
    # ideally, store this long token in a GitHub Secret (API_TOKEN)
    # and access it via: os.environ.get("API_TOKEN")
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImVmOWVmM2YzLWZhY2YtNGJlYi04ZGMyLTRkNTIwZTYyNjIzYSIsImVtYWlsIjoib2Rhdmllc0ByZWFkd3JpdGVkcy5jb20iLCJyb2xlIjoiQURNSU4iLCJpc0VtYWlsVmVyaWZpZWQiOnRydWUsImlhdCI6MTc2NDA2MjQ1NCwiZXhwIjoxNzY0MDYzMzU0fQ.tBsIdsIyD66KHt7ogll9uI6kQJfsTtNQhpiXb3CwVug"
    
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # 2. FILE CONFIGURATION
    # The key is likely 'file' based on standard APIs. 
    # If your API expects 'image' or 'media', change the first string below.
    files = [
        ('File', (filename, image_bytes, 'image/png')) 
    ]

    try:
        # 3. SEND REQUEST
        response = requests.post(url, headers=headers, files=files)
        
        # 4. HANDLE RESPONSE
        if response.status_code == 200 or response.status_code == 201:
            print(f"  -> Upload Success: {filename}")
            
            # CRITICAL: Adjust this line to match your API's JSON response
            # Run the request once in Postman to see where the URL is hidden.
            # Common patterns:
            # return response.json().get('url')
            # return response.json()['data']['url']
            # return response.json()['secure_url']
            
            # For now, I will guess it is at the root 'url' or 'data':
            data = response.json()
            if 'url' in data: return data['url']
            if 'data' in data and 'url' in data['data']: return data['data']['url']
            
            # Fallback if we can't find the key
            print(f"  -> Warning: Key 'url' not found in response: {data}")
            return None
            
        else:
            print(f"  -> API Error ({response.status_code}): {response.text}")
            return None
            
    except Exception as e:
        print(f"  -> Upload Failed: {e}")
        return None
def find_image_below_text(doc, text_query):
    query_short = str(text_query)[:100]
    best_match_page = -1
    best_rect = None
    highest_ratio = 0
    
    for page_num, page in enumerate(doc):
        text_blocks = page.get_text("blocks")
        for block in text_blocks:
            ratio = fuzz.partial_ratio(query_short, block[4])
            if ratio > 85 and ratio > highest_ratio:
                highest_ratio = ratio
                best_match_page = page_num
                best_rect = fitz.Rect(block[:4])

    if best_match_page == -1: return None

    page = doc[best_match_page]
    images = page.get_images(full=True)
    text_bottom = best_rect.y1
    candidate_xref, min_dist = None, 1000
    
    for img in images:
        xref = img[0]
        rects = page.get_image_rects(xref)
        if not rects: continue
        if rects[0].y0 >= text_bottom:
            dist = rects[0].y0 - text_bottom
            if dist < min_dist:
                min_dist = dist
                candidate_xref = xref

    if candidate_xref:
        base = doc.extract_image(candidate_xref)
        return {"bytes": base["image"], "ext": base["ext"]}
    return None

def main():
    # Args: [1]=ExcelInput, [2]=SourcePDF, [3]=OutputJSON
    try:
        df = pd.read_excel(sys.argv[1])
        questions = df.to_dict(orient='records')
    except:
        print("Error reading Excel input")
        return

    doc = fitz.open(sys.argv[2])
    lookup_map = {}

    for i, q in enumerate(questions):
        has_img = str(q.get('has_image', False)).lower() in ['true', '1']
        if has_img:
            img_data = find_image_below_text(doc, q.get('Question', ''))
            if img_data:
                url = upload_to_cloudflare(img_data['bytes'], f"q_{i}.{img_data['ext']}")
                if url: lookup_map[q.get('Question', '')] = url

    with open(sys.argv[3], 'w', encoding='utf-8') as f:
        json.dump(lookup_map, f, indent=4)

if __name__ == "__main__":
    main()
