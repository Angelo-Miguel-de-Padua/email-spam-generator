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
 
        }