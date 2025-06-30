from .templates import ham_senders, spam_senders, ham_templates, spam_templates, variables

class EmailDatasetGenerator:
    def __init__(self):
        self.ham_senders = ham_senders
        self.spam_senders = spam_senders
        self.ham_templates = ham_templates
        self.spam_templates = spam_templates
        self.variables = variables