from GetFileAndPeriod import *
class GetInputReport(GetFileAndPeriod):
    def __init__(self,ParentApp=None):
        super().__init__(ParentApp)
        self.ReportFilePath = None
        self.InputReport = None

    def _show_file_dialog_instructions(self):
        """This runs in the main thread"""
        instructions = (
            "Please select Reconcile Report files"
        )
        messagebox.showinfo("Instructions", instructions)
        
        # Start file selection process
        self.parent_app.root.after(0, self._select_files)

    def _select_files(self):
        """This runs in the main thread"""
        self.update_progress(20 , f"Selecting Reconcile Report file...")
            
        file_path = filedialog.askopenfilename(
            parent=self.parent_app.root,
            title=f"Select file for Reconcile Report",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]
        )
            
        if not file_path:
            messagebox.showerror("Error", "You must select all 4 files. Process cancelled.")
            self.result_queue.put(Exception("File selection cancelled."))
            return
            
        self.ReportFilePath = file_path
        self.log_message(f"Selected file for Reconcile Report File: {file_path}")
        
        self.files_selected.set()

    def getFile(self):
        self.getFilePath()
        self.InputReport = pd.read_excel(
            self.ReportFilePath,
            sheet_name="Reconcile Report",
            dtype={'Cost Center':str, 'LOB':str}
        )
        MiniGTO = pd.read_excel(
            self.ReportFilePath,
            sheet_name="Refund & Minimum",
            dtype={'Cost Center':str}
        )
        # Check if we got an error
        if not self.result_queue.empty():
            error = self.result_queue.get()
            if isinstance(error, Exception):
                raise error
        
        self.update_progress(70, "Loading Reconcile file...")

    
        self.log_message("input files loaded successfully")
        return self.InputReport, MiniGTO