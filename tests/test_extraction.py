import unittest
from bs4 import BeautifulSoup
from dags.job_business_extract import extract_entities_spacy, clean_name, extract_meta_info, extract_business_info_from_soup

class TestExtraction(unittest.TestCase):
    def test_nlp_extraction(self):
        # Mock soup object with some text
        html = """
        <html>
            <body>
                <footer>
                    &copy; 2023 Acme Corp. All rights reserved.
                </footer>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        # We need to mock the behavior of extract_business_info_from_soup or just test extract_entities_spacy directly
        # But extract_entities_spacy takes text, not soup.
        # Let's test extract_business_info_from_soup which uses spacy internally now
        results = extract_business_info_from_soup(soup)
        
        # Check if "Acme Corp" was extracted (either by regex or NLP)
        # Note: clean_name might strip "Corp" so we check for "acme" or "acme corp"
        company_names = [c['matched_text'] for c in results['CompanyName']]
        # The new logic might return "acme" if "Corp" is stripped
        self.assertTrue(any("Acme" in name for name in company_names), "Failed to extract company name")

    def test_clean_name(self):
        raw = "Acme Corp Australia Pty Ltd"
        cleaned = clean_name(raw)
        # "Corp", "Australia", "Pty Ltd" are all stripped
        self.assertEqual(cleaned, "acme")

    def test_meta_tag_extraction(self):
        html = """
        <html>
            <head>
                <meta property="og:site_name" content="Meta Corp" />
            </head>
            <body>
                <footer>
                    &copy; 2023 Footer Inc.
                </footer>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        meta_info = extract_meta_info(soup)
        self.assertEqual(meta_info["site_name"], "Meta Corp")

if __name__ == '__main__':
    unittest.main()
