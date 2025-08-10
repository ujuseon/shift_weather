from index import fetch_data, inch_to_mm, fahrenheit_to_celsius, knots_to_mps, ft_to_m
from unittest.mock import patch

# Проверки фукнций преобразования единиц измерения
def test_inch_to_mm():
    assert inch_to_mm(0) == 0
    assert inch_to_mm(1) == 25.4
    assert inch_to_mm(0.5) == 12.7

def test_fahrenheit_to_celsius():
    assert fahrenheit_to_celsius(32) == 0
    assert fahrenheit_to_celsius(212) == 100
    assert round(fahrenheit_to_celsius(68), 2) == 20

def test_knots_to_mps():
    assert knots_to_mps(0) == 0
    assert round(knots_to_mps(1), 5) == 0.51444
    assert round(knots_to_mps(10), 5) == 5.14444

def test_ft_to_m():
    assert ft_to_m(0) == 0
    assert round(ft_to_m(1), 5) == 0.3048
    assert round(ft_to_m(10), 5) == 3.048

# Функция проверки того, что fetch_data возвращает json
def test_fetch_data_returns_json():
    url = "https://api.open-meteo.com/v1/forecast?latitude=35.6895&longitude=139.6917&current_weather=true"
    
    # Имитация успешного ответа API
    mock_response = {
        "current_weather": {
            "temperature": 20,
            "windspeed": 5,
            "weathercode": 1
        }
    }
    
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        result = fetch_data(url)
        assert isinstance(result, dict)
