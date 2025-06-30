from email_generator.generator import EmailDatasetGenerator

def create_comprehensive_dataset():
    generator = EmailDatasetGenerator()
    dataset = generator.generate_dataset(2000, spam_ratio=0.5)
    generator.preview_dataset(dataset)
    df = generator.save_to_csv(dataset, 'synthetic_email_dataset.csv')
    generator.analyze_dataset(dataset)