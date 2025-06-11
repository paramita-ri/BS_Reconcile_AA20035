from GetReport import *
class GenNewReport(GetReport):
    def __init__(self, InputReport, MiniGTO_df, Parent_app=None):
        super().__init__(None, None, None,None, Parent_app)
        self.InputReport = InputReport
        self.MiniGTO_df = MiniGTO_df
    
    def getNewReconcile(self):
        if self.InputReport is None:
            raise ValueError("InputReport cannot be None")
        self.log_message("Processing InputReport...")
        NewReconcile = self.InputReport.copy()
        NewReconcile['Cost Center'] = NewReconcile['Cost Center'].astype(str).str.zfill(5)
    
        NewReconcile['Transaction Date'] = pd.to_datetime(
            NewReconcile['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )
        NewReconcile = NewReconcile.sort_values(by=['Cost Center', 'Transaction Date'])
    
        NewReconcile['Period'] = pd.to_datetime(
            NewReconcile['Period'],
            format='%d/%m/%Y', errors='coerce'
        )
        NewReconcile = NewReconcile.reset_index(drop=True) 
        self.NewReconcile = NewReconcile
        self.log_message("New reconciliation report created")
        return self.NewReconcile  
    
    def getReport(self):
        self.update_progress(95, "Preparing reports...")
        
        newReconcile = self.getNewReconcile()
        groupby = self.getGroupby()
        onlypending = self.getOnlyPending()
        onlyminimum = self.getOnlyMinimum()
        MiniGTO_df = self.MiniGTO_df
        MiniGTO_df['Transaction Date'] = pd.to_datetime(
            MiniGTO_df['Transaction Date'],
            format='%d/%m/%Y', errors='coerce'
        )


        all_dfs = {}
        if newReconcile is not None:
            all_dfs["Reconcile Report"] = newReconcile
        if groupby is not None:
            all_dfs["Summary Current Month"] = groupby
        if onlypending is not None:
            all_dfs["Food Courts Pending Bills"] = onlypending
        if onlyminimum is not None:
            all_dfs["Minimum Guarantee"] = onlyminimum
        if MiniGTO_df is not None:
            all_dfs["Refund & Minimum"] = MiniGTO_df

        if all_dfs:
            saved_path = self.save_to_excel(all_dfs, "Save All Reports", "ReconcileReport.xlsx")
            if saved_path:
                self.log_message(f"Reports successfully saved to: {saved_path}")
            else:
                self.log_message("Report saving was cancelled")
        
        self.update_progress(100, "Reports generation complete")  