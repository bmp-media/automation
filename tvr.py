from modules.paths import PathManager
from modules.logics import ProcessingManager
from modules.workbook import WorkbookManager
from modules.dataframe import DataframeManager


def main() -> None:

    try:
        file_paths = PathManager.open_file_dialog()
        workbook = WorkbookManager.create_workbook()

        counter = 1

        for file in file_paths:

            try:
                print(f'Обрабатывается файл: {file}')

                df = DataframeManager.import_dataframe(file)
                df = ProcessingManager.tvr(df)

                sheet_name = f'{df["target"]} - {counter}'
                sheet = WorkbookManager.create_sheet(workbook, sheet_name)

                counter += 1

                DataframeManager.export_dataframe_to_sheet(df['dataframe'], sheet)

                WorkbookManager.format_sheet(sheet)

            except Exception as e:
                print(f'Ошибка при обработке файла: {file}')
                print(f'Тип ошибки: {type(e).__name__}')
                print(f'Сообщение об ошибке: {str(e)}')

                continue

        name = PathManager.save_file_dialog()

        WorkbookManager.save_workbook(workbook, name)

    except Exception as e:
        print('Произошла ошибка во время выполнения программы.')
        print(f'Тип ошибки: {type(e).__name__}')
        print(f'Сообщение об ошибке: {str(e)}')


if __name__ == '__main__':
    main()
