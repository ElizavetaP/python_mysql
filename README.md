Программа использует данные о погоде с сайта http://openweathermap.org/, либо из файла. 
Записывает данные в базу данных MySql. 
Программа использует config.ini для подключения к бд.
В параметрах командной строки скрипту надо режим работы (import - импортирование из файла, site - импортирование с сайта, 
average - средняя температура за интересующий интервал дат, daycalc - средняя температура за день для интересующей станции, 
hottest - станция с самым жарким прогнозом за сегодня, all - вывод всех данных за сегодня, history).
Вывод осуществляется в единицах измерения из config.ini.
