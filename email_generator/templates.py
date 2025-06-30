ham_senders = {
    'education': [
        'registrar@university.edu',
        'admin@college.edu',
        'info@onlinecourse.com',
        'support@coursera.org',
        'office@edx.org'
    ],
    'ecommerce': [
        'noreply@amazon.com',
        'orders@shopee.ph',
        'sales@lazada.com',
        'support@ebay.com',
        'noreply@bestbuy.com'
    ],
    'finance': [
        'alerts@bdo.com.ph',
        'support@bpi.com.ph',
        'noreply@paypal.com',
        'billing@revolut.com',
        'info@unionbankph.com'
    ],
    'social': [
        'updates@facebookmail.com',
        'notifications@twitter.com',
        'noreply@linkedin.com',
        'hello@discord.com',
        'support@tiktok.com'
    ],
    'work': [
        'hr@company.com',
        'team@startup.io',
        'itdept@corporate.com',
        'manager@projecthub.org',
        'careers@enterprise.net'
    ]
}

spam_senders = {
    'lottery': [
        'lottery@winners.com',
        'claim@jackpot.com',
        'notifier@bigwin.site',
        'info@cashalert.win',
        'official@prizecenter.net'
    ],
    'investment': [
        'advisor@quickprofits.biz',
        'crypto@blockgain.top',
        'returns@investzone.site',
        'account@fxboost.click',
        'broker@tradeking.pro'
    ],
    'phishing': [
        'security@paypal-alert.com',
        'verify@apple-login.net',
        'reset@bank-secure.com',
        'support@amazon-alerts.biz',
        'login@facebook-update.info'
    ],
    'fake_jobs': [
        'hr@globalrecruiters.site',
        'career@dreamjobs.click',
        'jobs@staffingworld.top',
        'apply@nextgenjobs.online',
        'recruit@homehiring.pro'
    ]
}

ham_templates = {
    'order': [
        (
            "Order Confirmation - #{order_id}",
            "Thank you for your purchase of ₱{amount}. Your order #{order_id} will be shipped to {location}."
        ),
        (
            "Your recent purchase from {company}",
            "Your payment of ₱{amount} has been received. Estimated delivery: {date}, {time}."
        )
    ],
    'job': [
        (
            "Interview for {position} at {company}",
            "You are invited for an interview on {date} at {time}. Please confirm your attendance."
        ),
        (
            "Application Received - {position}",
            "Thank you for applying to {company}. We are reviewing your application."
        )
    ],
    'edu': [
        (
            "Your course update: {course}",
            "Your next lesson is scheduled on {date} via {device}. Please be prepared."
        ),
        (
            "Grades released for {course}",
            "Your final grade in {course} is now available. Please log in to your {company} account."
        )
    ]
}

spam_templates = {
    'lottery': [
        (
            "Congratulations! You've won ₱{amount}!",
            "Your email was selected in our {country} promo. Claim ₱{amount} in just {hours} hours!"
        ),
        (
            "Final Notice: ₱{amount} awaiting claim",
            "You have {hours} hours to respond and claim your reward from {location}."
        )
    ],
    'phishing': [
        (
            "URGENT: Action required on your {company} account",
            "We detected unusual activity. Please verify your login within {hours} or access will be restricted."
        ),
        (
            "Payment Failed - Immediate Attention Needed",
            "Your recent transaction of ₱{amount} was declined. Update your billing info now to avoid penalties."
        )
    ],
    'investment': [
        (
            "Earn ₱{price} in just {days} days!",
            "Our top clients are earning daily returns of ₱{daily_rate}. Join now before slots close!"
        ),
        (
            "Double your income from home!",
            "Invest ₱{amount} and get {percent}% return in 1 week. Trusted by thousands in {country}."
        )
    ],
    'jobs': [
        (
            "Work from home ₱{hourly}/hr! No experience needed!",
            "Start earning ₱{hourly}/hr by completing simple tasks from {location}. Sign up now!"
        ),
        (
            "We're hiring urgently! ₱{daily_rate} per day",
            "Positions open in {country}. Limited slots. Training provided. Apply now!"
        )
    ]
}
