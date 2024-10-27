
extractor = HolidayExtractor(2024)
national = extractor.get_national_holidays()
state = extractor.get_state_holidays()

print(f"Nacionais: {len(national)}")
print(f"Estaduais: {len(state)}")