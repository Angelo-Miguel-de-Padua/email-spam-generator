import random
from faker import Faker
from datetime import datetime, timedelta
from .templates import ham_senders, spam_senders, ham_templates, spam_templates, variables

class EmailDatasetGenerator:
    def __init__(self):
        self.ham_senders = ham_senders
        self.spam_senders = spam_senders
        self.ham_templates = ham_templates
        self.spam_templates = spam_templates
        self.variables = variables
        self.faker = Faker()
    
    def generate_ham_email(self):
        category = random.choice(list(self.ham_senders.keys()))
        sender = random.choice(self.ham_senders[category])

        template_category = random.choice(list(self.ham_templates.keys()))
        subject_template, content_template = random.choice(self.ham_templates[template_category])

        template_vars = {
            'device': f"{self.faker.user_agent()}",
            'date': self.faker.date_between(start_date='30d', end_date='today').strftime('%B %d, %Y'),
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