import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from .spam_utils import add_spam_characteristics 
from .templates import ham_senders, spam_senders, ham_templates, spam_templates

class EmailDatasetGenerator:
    def __init__(self):
        self.ham_senders = ham_senders
        self.spam_senders = spam_senders
        self.ham_templates = ham_templates
        self.spam_templates = spam_templates
        self.faker = Faker()
    
    def generate_ham_email(self):
        category = random.choice(list(self.ham_senders.keys()))
        sender = random.choice(self.ham_senders[category])

        template_category = random.choice(list(self.ham_templates.keys()))
        subject_template, content_template = random.choice(self.ham_templates[template_category])

        template_vars = {
            'device': f"{self.faker.user_agent()}",
            'date': self.faker.date_between(start_date=datetime.today() - timedelta(days=30), end_date=datetime.today()).strftime('%B %d, %Y'),
            'time': self.faker.time(pattern="%I:%M %p"),
            'company': self.faker.company(),
            'position': self.faker.job(),
            'course': self.faker.catch_phrase(),
            'field': self.faker.bs().split()[-1],
            'hours': random.randint(5, 40),
            'points': random.randint(50, 500),
            'amount': random.randint(10, 999),
            'transactions': random.randint(3, 20),
            'percent': random.randint(60, 99),
            'order_id': random.randint(100000, 999999),
            'last4': random.randint(1000, 9999),
            'location': f"{self.faker.city()}, {self.faker.country()}"
        }

        try:
            subject = subject_template.format(**template_vars)
            content = content_template.format(**template_vars)
        except KeyError as e:
            print(f"Missing key in template: {e}")
            subject = subject_template
            content = content_template

        return {
            'subject': subject,
            'sender': sender,
            'content': content,
            'label': 'ham',
            'sender_domain': sender.split('@')[1] if '@' in sender else 'unknown'
        }
    
    def generate_spam_email(self):
        category = random.choice(list(self.spam_senders.keys()))
        use_spoofed_gender = random.random() < 0.5

        if use_spoofed_gender:
            tlds = ['.click', '.top', '.biz', '.site', '.online', '.info']
            keywords = ['login', 'update', 'account', 'secure', 'verify', 'alert', 'billing', 'support']

            word1 = random.choice(keywords)
            word2 = random.choice(keywords)
            tld = random.choice(tlds)

            spoofed_domain = f"{word1}-{word2}{tld}"
            sender = f"{self.faker.user_name()}@{spoofed_domain}"
            is_spoofed = True
        else:
            sender = random.choice(self.spam_senders[category])
            is_spoofed = False

        template_category = random.choice(list(self.spam_templates.keys()))
        subject_template, content_template = random.choice(self.spam_templates[template_category])

        template_vars = {
            'location': f"{self.faker.city()}, {self.faker.country()}",
            'num': random.randint(3, 50),
            'hours': random.randint(6, 48),
            'amount': self.faker.random_int(min=500, max=50000),
            'price': self.faker.random_int(min=10000, max=1000000),
            'percent': self.faker.random_int(min=1000, max=10000),
            'daily_rate': self.faker.random_int(min=200, max=2000),
            'hourly': self.faker.random_int(min=25, max=200),
            'fee': round(random.uniform(2.99, 19.99), 2),
            'days': self.faker.random_int(min=3, max=30),
            'country': self.faker.country(),
            'name': self.faker.name()
        }
    
        try:
            subject = subject_template.format(**template_vars)
            content = content_template.format(**template_vars)
        except KeyError:
            subject = subject_template
            content = content_template
        
        if random.random() < 0.5:
            subject = add_spam_characteristics(subject)
            content = add_spam_characteristics(content)

        return {
            'subject': subject,
            'sender': sender,
            'content': content,
            'label': 'spam',
            'sender_domain': sender.split('@')[1] if '@' in sender else 'unknown',
            'is_spoofed': is_spoofed
        }
    
    def generate_dataset(self, total_emails=1000, spam_ratio=0.5):
        dataset = []
        num_spam = int(total_emails * spam_ratio)
        num_ham = total_emails - num_spam

        for _ in range(num_ham):
            dataset.append(self.generate_ham_email())
        
        for _ in range (num_spam):
            dataset.append(self.generate_spam_email())
        
        random.shuffle(dataset)
        return dataset

    def save_to_csv(self, dataset, filename='synthetic_email_dataset.csv'):
        df = pd.DataFrame(dataset)
        df.to_csv(filename, index=False, encoding='utf-8')
        return df

    def preview_dataset(self, dataset, num_examples=10):
        for i, email in enumerate(dataset[:num_examples]):
            print(f"{i+1}. [{email['label'].upper()}] {email['sender']} - {email['subject']}")
    
    def analyze_dataset(self, dataset):
        df = pd.DataFrame(dataset)
        print(df['label'].value_counts())
        print(df['sender_domain'].value_counts().head())
        return df
