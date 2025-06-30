from email_generator.generator import EmailDatasetGenerator

def create_comprehensive_dataset():
    generator = EmailDatasetGenerator()
    dataset = generator.generate_dataset(2000, spam_ratio=0.5)
    generator.preview_dataset(dataset)
    df = generator.save_to_csv(dataset, 'synthetic_email_dataset.csv')
    generator.analyze_dataset(dataset)

    spam_heavy = generator.generate_dataset(500, spam_ratio=0.8)
    generator.save_to_csv(spam_heavy, 'spam_heavy_dataset.csv')

    ham_heavy = generator.generate_dataset(500, ham_ratio=0.8)
    generator.save_to_csv(ham_heavy, 'ham_heavy_dataset.csv')

if __name__ == "__main__":
    create_comprehensive_dataset()