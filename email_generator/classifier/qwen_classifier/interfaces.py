from email_generator.utils.domain_utils import is_valid_domain
from email_generator.classifier.security.cloud_metadata import check_domain_safety
from email_generator.utils.robots_util import is_scraping_allowed
from email_generator.utils.rate_limiter import apply_rate_limit, get_adaptive_delay

class DefaultValidator:
    def is_valid_domain(self, domain: str) -> bool:
        return is_valid_domain(domain)

    def check_domain_safety(self, domain: str) -> bool:
        return check_domain_safety(domain)

    def is_scraping_allowed(self, domain: str) -> bool:
        return is_scraping_allowed(domain)

class DefaultRateLimiter:
    def apply_rate_limit(self, domain: str) -> None:
        apply_rate_limit(domain)

    def get_adaptive_delay(self, had_error: bool, response_time: float = 0.0) -> float:
        return get_adaptive_delay(had_error, response_time)


