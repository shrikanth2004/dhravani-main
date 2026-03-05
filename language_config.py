"""Language configuration for the dataset preparation system"""

# LANGUAGES = {
#     'af': {'name': 'Afrikaans', 'native_name': 'Afrikaans'},
#     'ar': {'name': 'Arabic', 'native_name': 'العربية'},
#     'az': {'name': 'Azerbaijani', 'native_name': 'Azərbaycan'},
#     'be': {'name': 'Belarusian', 'native_name': 'Беларуская'},
#     'bg': {'name': 'Bulgarian', 'native_name': 'Български'},
#     'bn': {'name': 'Bengali', 'native_name': 'বাংলা'},
#     'bs': {'name': 'Bosnian', 'native_name': 'Bosanski'},
#     'ca': {'name': 'Catalan', 'native_name': 'Català'},
#     'cs': {'name': 'Czech', 'native_name': 'Čeština'},
#     'cy': {'name': 'Welsh', 'native_name': 'Cymraeg'},
#     'da': {'name': 'Danish', 'native_name': 'Dansk'},
#     'de': {'name': 'German', 'native_name': 'Deutsch'},
#     'el': {'name': 'Greek', 'native_name': 'Ελληνικά'},
#     'en': {'name': 'English', 'native_name': 'English'},
#     'es': {'name': 'Spanish', 'native_name': 'Español'},
#     'et': {'name': 'Estonian', 'native_name': 'Eesti'},
#     'eu': {'name': 'Basque', 'native_name': 'Euskara'},
#     'fa': {'name': 'Persian', 'native_name': 'فارسی'},
#     'fi': {'name': 'Finnish', 'native_name': 'Suomi'},
#     'fr': {'name': 'French', 'native_name': 'Français'},
#     'ga': {'name': 'Irish', 'native_name': 'Gaeilge'},
#     'gl': {'name': 'Galician', 'native_name': 'Galego'},
#     'gu': {'name': 'Gujarati', 'native_name': 'ગુજરાતી'},
#     'he': {'name': 'Hebrew', 'native_name': 'עברית'},
#     'hi': {'name': 'Hindi', 'native_name': 'हिन्दी'},
#     'hr': {'name': 'Croatian', 'native_name': 'Hrvatski'},
#     'hu': {'name': 'Hungarian', 'native_name': 'Magyar'},
#     'hy': {'name': 'Armenian', 'native_name': 'Հայերեն'},
#     'id': {'name': 'Indonesian', 'native_name': 'Indonesia'},
#     'is': {'name': 'Icelandic', 'native_name': 'Íslenska'},
#     'it': {'name': 'Italian', 'native_name': 'Italiano'},
#     'ja': {'name': 'Japanese', 'native_name': '日本語'},
#     'jv': {'name': 'Javanese', 'native_name': 'Basa Jawa'},
#     'ka': {'name': 'Georgian', 'native_name': 'ქართული'},
#     'kk': {'name': 'Kazakh', 'native_name': 'Қазақша'},
#     'km': {'name': 'Khmer', 'native_name': 'ខ្មែរ'},
#     'kn': {'name': 'Kannada', 'native_name': 'ಕನ್ನಡ'},
#     'ko': {'name': 'Korean', 'native_name': '한국어'},
#     'ky': {'name': 'Kyrgyz', 'native_name': 'Кыргызча'},
#     'la': {'name': 'Latin', 'native_name': 'Latina'},
#     'lt': {'name': 'Lithuanian', 'native_name': 'Lietuvių'},
#     'lv': {'name': 'Latvian', 'native_name': 'Latviešu'},
#     'mk': {'name': 'Macedonian', 'native_name': 'Македонски'},
#     'ml': {'name': 'Malayalam', 'native_name': 'മലയാളം'},
#     'mn': {'name': 'Mongolian', 'native_name': 'Монгол'},
#     'mr': {'name': 'Marathi', 'native_name': 'मराठी'},
#     'ms': {'name': 'Malay', 'native_name': 'Bahasa Melayu'},
#     'my': {'name': 'Burmese', 'native_name': 'မြန်မာဘာသာ'},
#     'ne': {'name': 'Nepali', 'native_name': 'नेपाली'},
#     'nl': {'name': 'Dutch', 'native_name': 'Nederlands'},
#     'no': {'name': 'Norwegian', 'native_name': 'Norsk'},
#     'pa': {'name': 'Punjabi', 'native_name': 'ਪੰਜਾਬੀ'},
#     'pl': {'name': 'Polish', 'native_name': 'Polski'},
#     'pt': {'name': 'Portuguese', 'native_name': 'Português'},
#     'ro': {'name': 'Romanian', 'native_name': 'Română'},
#     'ru': {'name': 'Russian', 'native_name': 'Русский'},
#     'si': {'name': 'Sinhala', 'native_name': 'සිංහල'},
#     'sk': {'name': 'Slovak', 'native_name': 'Slovenčina'},
#     'sl': {'name': 'Slovenian', 'native_name': 'Slovenščina'},
#     'sq': {'name': 'Albanian', 'native_name': 'Shqip'},
#     'sr': {'name': 'Serbian', 'native_name': 'Српски'},
#     'su': {'name': 'Sundanese', 'native_name': 'Basa Sunda'},
#     'sv': {'name': 'Swedish', 'native_name': 'Svenska'},
#     'sw': {'name': 'Swahili', 'native_name': 'Kiswahili'},
#     'ta': {'name': 'Tamil', 'native_name': 'தமிழ்'},
#     'te': {'name': 'Telugu', 'native_name': 'తెలుగు'},
#     'tg': {'name': 'Tajik', 'native_name': 'Тоҷикӣ'},
#     'th': {'name': 'Thai', 'native_name': 'ไทย'},
#     'tl': {'name': 'Filipino', 'native_name': 'Filipino'},
#     'tr': {'name': 'Turkish', 'native_name': 'Türkçe'},
#     'uk': {'name': 'Ukrainian', 'native_name': 'Українська'},
#     'ur': {'name': 'Urdu', 'native_name': 'اردو'},
#     'uz': {'name': 'Uzbek', 'native_name': "O'zbek"},
#     'vi': {'name': 'Vietnamese', 'native_name': 'Tiếng Việt'},
#     'zh': {'name': 'Chinese', 'native_name': '中文'},
# }

LANGUAGES = {
    'as': {'name': 'Assamese', 'native_name': 'অসমীয়া'},
    'bn': {'name': 'Bengali', 'native_name': 'বাংলা'},
    'gu': {'name': 'Gujarati', 'native_name': 'ગુજરાતી'},
    'hi': {'name': 'Hindi', 'native_name': 'हिन्दी'},
    'kn': {'name': 'Kannada', 'native_name': 'ಕನ್ನಡ'},
    'ml': {'name': 'Malayalam', 'native_name': 'മലയാളം'},
    'mr': {'name': 'Marathi', 'native_name': 'मराठी'},
    'ne': {'name': 'Nepali', 'native_name': 'नेपाली'},
    'or': {'name': 'Odia', 'native_name': 'ଓଡ଼ିଆ'},
    'pa': {'name': 'Punjabi', 'native_name': 'ਪੰਜਾਬੀ'},
    'sa': {'name': 'Sanskrit', 'native_name': 'संस्कृतम्'},
    'ta': {'name': 'Tamil', 'native_name': 'தமிழ்'},
    'te': {'name': 'Telugu', 'native_name': 'తెలుగు'},
    'ur': {'name': 'Urdu', 'native_name': 'اردو'},
    'en': {'name': 'English', 'native_name': 'English'},
    'do': {'name': 'Dogri', 'native_name': 'डोगरी'}
}


def get_language_name(code):
    """Get the English name of a language from its code"""
    return LANGUAGES.get(code, {}).get('name', code)

def get_native_name(code):
    """Get the native name of a language from its code"""
    return LANGUAGES.get(code, {}).get('native_name', code)

def get_language_code(name):
    """Get the language code from its English name"""
    for code, lang in LANGUAGES.items():
        if lang['name'].lower() == name.lower():
            return code
    return None

def get_all_languages():
    """Get a list of all supported languages"""
    return [{'code': code, **lang} for code, lang in LANGUAGES.items()] 