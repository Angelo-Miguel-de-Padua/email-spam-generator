import random
import re

def add_spam_characteristics(text, probability=0.3):
    if random.random() > probability:
        return text
    
    if random.random() < 0.4:
        text = re.sub(r'!', '!!!', text)
        text = re.sub(r'\?', '???', text)

    if random.random() < 0.3:
        typo_chars = {'e': '3', 'a': '@', 'o': '0', 's': '$', 'i': '1'}
        for original, replacement in typo_chars.items():
            if random.random() < 0.2:
                text = text.replace(original, replacement, 1)

    if random.random() < 0.3:
        words = text.split()
        for i in range(len(words)):
            if random.random() < 0.3:
                words[i] = words[i].upper()
        text = ' '.join(words)

    return text
