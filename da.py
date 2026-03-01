import requests
from bs4 import BeautifulSoup

def decode_secret_message(doc_url):
    response = requests.get(doc_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    rows = soup.find_all("tr")[1:]
    points = []
    max_x = max_y = 0
    for row in rows:
        cols = row.find_all("td")
        x = int(cols[0].text)
        char = cols[1].text
        y = int(cols[2].text)
        points.append((x, y, char))
        max_x = max(max_x, x)
        max_y = max(max_y, y)
    print_message(points,max_x,max_y)

def print_message(points,max_x,max_y):
    grid = [[" " for _ in range(max_x + 1)] for _ in range(max_y + 1)]

    for x, y, char in points:
        grid[y][x] = char

    for row in grid:
        print("".join(row))

decode_secret_message(url)