import tkinter as tk
from tkinter import filedialog


class PathManager:

    @staticmethod
    def open_file_dialog():
        root = tk.Tk()
        root.withdraw()
        file_paths = filedialog.askopenfilenames()
        return file_paths

    @staticmethod
    def open_file_dialog_solo():
        root = tk.Tk()
        root.withdraw()
        file_paths = filedialog.askopenfilename()
        return file_paths

    @staticmethod
    def save_file_dialog():
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.asksaveasfilename(defaultextension='.xlsx')
        return file_path

    @staticmethod
    def save_in_directory_dialog():
        root = tk.Tk()
        root.withdraw()
        file_path = filedialog.askdirectory()
        return file_path
