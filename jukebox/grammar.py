import sys

def strip_articles(text):
    new_text = text.lower()
    words = new_text.split(" ")

    articles = ["a", "an", "the"]
    for article in articles:
        words = [word for word in words if word != article]

    return " ".join(words).strip()
