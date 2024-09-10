# Информация об облигациях на MOEX
Набор функций, позволяющих получить исчерпывающую информацию об облигациях, торгующихся на Московской бирже, путем обработки данных ISS API MOEX. Основная функция *moex_bond_info* возвращает два датафрейма с данными. Первый содержит основные характеристики выпуска и итоги торгов финансовым инструментом. В аналогичном составе информация по отдельности доступна, например, на сайте [Московской биржи](https://www.moex.com/ru/issue.aspx?board=TQCB&code=RU000A105U00) или [RusBonds](https://rusbonds.ru/bonds/226684/). Второй содержит график денежных потоков, включая купоны, амортизацию долга и оферты, предусмотренные проспектом ценных бумаг. Аналогичная информация по отдельности доступна, например, на сайте Финам (подразделы ["Платежи"](https://bonds.finam.ru/issue/details0267000002/default.asp) и ["Оферты"](https://bonds.finam.ru/issue/details0267000003/default.asp)).

Примеры использования функций представлены в ноутбуке *moex_bonds_tut* и включают в себя:
1. Расчет доходности на текущую дату для простых облигаций;
2. Генерацию [HTML отчета](https://html-preview.github.io/?url=https://github.com/cyril-dv/moex_bonds/blob/main/bond_reports/%D0%93%D0%B0%D0%B7%D0%BF%D1%80%D0%BE%D0%BC%D0%9AP8%2C%20RU000A105U00.html) для последующего использования в электронном или печатном виде.

Зависимости:
* requests
* pandas
* jinja2 (генерация отчетов)
* pyxirr (расчет доходности)
