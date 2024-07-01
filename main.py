import platform
import subprocess
import sys
import time
from colorama import init, Fore, Style

if platform.system() == 'Windows':
    init(convert=True)


def run_script(script_name):
    try:
        if platform.system() == 'Windows':
            subprocess.run(['python', script_name], shell=True)
        else:
            subprocess.run(['python3', script_name])
    except Exception as e:
        print(f'Ошибка при выполнении скрипта {script_name}: {e}')


def print_slowly(text):
    for char in text:
        sys.stdout.write(Fore.GREEN + char + Style.RESET_ALL)
        sys.stdout.flush()
        time.sleep(0.01)


def print_menu():
    print_slowly('Добро пожаловать в программу!\n')
    print_slowly('Выберите скрипт для запуска:\n')


def print_choice():
    print()
    print_slowly('Дополнительные выгрузки:\n\n')
    print_slowly('1. Выгрузки через API\n')
    print_slowly('2. Reach\n')
    print_slowly('Основные выгрузки\n\n')
    print_slowly('3. Национальная выгрузка\n')
    print_slowly('4. Региональная выгрузка\n')
    print_slowly('5. Выгрузка Москва ООН\n\n')

    print_slowly('Опционально\n\n')
    print_slowly('6. Сравнение Excel файлов\n\n')
    print_slowly('0. Выход\n')


def print_instructions():
    print_slowly('\nВведите номер скрипта, который вы хотите запустить.\n')
    print_slowly('Для выхода из программы введите 0.\n')


def main():

    scripts = {
        '1': 'api.py',
        '2': 'reach.py',
        '3': 'national.py',
        '4': 'regional.py',
        '5': 'regional moscow.py',
        '6': 'excel.py',
    }

    print_menu()
    print_choice()
    print_instructions()

    while True:

        print_slowly('\nВведите номер скрипта: ')

        choice = input()

        if choice in scripts:
            script_name = scripts[choice]
            run_script(script_name)
        elif choice == '0':
            break
        else:
            print_slowly('\nНеверный ввод. Пожалуйста, выберите номер из меню.\n')

        print_choice()


if __name__ == '__main__':
    main()
