from GetFileAndPeriod import * #Import all class
from AAProcess import *
from Combine import *
from PendingBills import *
from Balance import *
from Minimum import *
from GetReport import *

#ฟังก์ชันแสดง dataframe 25 row แรก
def display_df(name, df):
    if df is not None:
        print(f"\n{'-'*60}\n{name} (first 25 rows):\n{'-'*60}")
        print(df.head(25))
        print(f"\nShape: {df.shape} (rows, columns)")
        print(f"Columns: {list(df.columns)}")
        print("-"*60 + "\n")

def save_to_excel(df, title, default_name):
    save_path = filedialog.asksaveasfilename(
        title=title,
        initialfile=default_name,
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )

    if save_path:
        try:
            df.to_excel(save_path, index=False)
            messagebox.showinfo("Success", f"File saved successfully:\n{save_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")
        
# Main เพื่อเริ่มการทำงาน        
if __name__ == "__main__":
    # รับ input file และ input period
    GetInput = GetFileAndPeriod()
    Period_date = GetInput.getPeriod()
    AAcurrent_df, AAprevious_df,AAStar_df, LastReconcile_df, GTO05_df = GetInput.getFile()
    print(Period_date)
    display_df("AAcurrent_df", AAcurrent_df)
    display_df("AAprevious_df", AAprevious_df)
    display_df("AAStar_df", AAStar_df)
    display_df("LastReconcile_df", LastReconcile_df)
    display_df("GTO05_df", GTO05_df)

   #นำ AAcurrent_df, GTO05_df, Period_date ไปใช้เพื่อทำ dataframe จาก AA เพื่อรวมกับ LastReconcile_df
    AA_Process = AAProcess(AAcurrent_df, GTO05_df, Period_date)
    AAPivot_df, AAForCombine_df = AA_Process.getAApivot()
    display_df("AAPivot_df", AAPivot_df)
    display_df("AAForCombine_df", AAForCombine_df)
    save_to_excel(AAPivot_df, "Save AAPivot_df", "AAPivot_df.xlsx")
    save_to_excel(AAForCombine_df, "Save AAForCombine_df", "AAForCombine_df.xlsx")
    
    # รวม AAForCombine_df และ LastReconcile_df เพื่อใช้ในการทำ reconcile
    combine = Combine(AAForCombine_df, LastReconcile_df)
    Combined_df = combine.getCombine_df()
    display_df("Combined_df", Combined_df)
    save_to_excel(Combined_df, "Save Combined_df", "Combined_df.xlsx")
    # แยก table ที่มีเฉพาะ row ที่เป็น group ยังไม่วางบิลใน period ปัจจุบัน ออกมา
    pendingbills = PendingBills(Combined_df, Period_date)
    Pending_df, ForBalance_df = pendingbills.getPendingBills()
    display_df("Pending_df", Pending_df)
    save_to_excel(Pending_df, "Save Pending_df", "Pending_df.xlsx")
    display_df("ForBalance_df", ForBalance_df)
    save_to_excel(ForBalance_df, "Save ForBalance_df", "ForBalance_df.xlsx") 
    # ทำการ balance โดยลบ row ที่สามารถรวมกันเป็น 0 ออกได้
    balance = Balance(ForBalance_df)
    Balanced_df = balance.getBalance()
    display_df("Balanced_df", Balanced_df)
    save_to_excel(Balanced_df, "Save Balanced_df", "Balanced_df.xlsx") 
    # 
    minimum = Minimum(Balanced_df, AAStar_df, GTO05_df)
    minimum_df = minimum.getMinimum_df()
    display_df("minimum_df", minimum_df)
    save_to_excel(minimum_df, "Save minimum_df", "minimum_df.xlsx") 
    display_df("toFindminimum_df", minimum.toFindminimum_df)
    save_to_excel(minimum.toFindminimum_df, "Save minimum.toFindminimum_df", "minimum.toFindminimum_df.xlsx") 
    # สร้าง report ไฟล์โดยมี่ไฟล์สรุปประจำเดือน, ไฟล์ minimum, ไฟล์ยังไม่วางบิล แล้ว save ลง excel
    getReport = GetReport(Pending_df, minimum_df)#Pending_df, Balanced_df
    getReport.getReport()
    display_df("Newreconcile", getReport.NewReconcile)
    display_df("Groupby", getReport.Groupby_df)
    display_df("Minimum", getReport.OnlyMinimum)
    display_df("PendingBills", getReport.OnlyPending)
    save_to_excel(getReport.NewReconcile, "Save NewReconcile", "NewReconcile.xlsx")
    save_to_excel(getReport.Groupby_df, "Save Groupby_df", "Groupby_df.xlsx")
    save_to_excel(getReport.OnlyMinimum, "Save OnlyMinimum", "OnlyMinimum.xlsx")
    save_to_excel(getReport.OnlyPending, "Save OnlyPending", "OnlyPending.xlsx")
