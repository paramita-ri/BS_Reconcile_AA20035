from GetFileAndPeriod import *
from AAProcess import *
from Combine import *
from PendingBills import *
from Balance import *
from Minimum import *
from GetReport import *
import tkinter as tk
from tkinter import ttk, messagebox
import time
import threading
import queue

class ReconciliationApp:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Balance Sheet Reconciliation Tool")
        self.root.geometry("800x600")
        
        # Create message queue for thread-safe communication
        self.message_queue = queue.Queue()
        
        # Create main container
        self.main_frame = ttk.Frame(self.root, padding="20")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Application title
        ttk.Label(self.main_frame, text="Balance Sheet Reconciliation", 
                 font=("Helvetica", 16, "bold")).pack(pady=10)
        
        # Status frame
        self.status_frame = ttk.LabelFrame(self.main_frame, text="Progress", padding=10)
        self.status_frame.pack(fill=tk.X, pady=10)
        
        self.status_label = ttk.Label(self.status_frame, text="Ready to start...")
        self.status_label.pack()
        
        self.progress = ttk.Progressbar(self.status_frame, mode='determinate')
        self.progress.pack(fill=tk.X)
        
        # Button frame
        self.button_frame = ttk.Frame(self.main_frame)
        self.button_frame.pack(pady=20)
        
        self.start_button = ttk.Button(self.button_frame, text="Start Reconciliation", 
                                      command=self.start_reconciliation)
        self.start_button.pack(pady=5)
        
        self.exit_button = ttk.Button(self.button_frame, text="Exit", 
                                    command=self.root.destroy)
        self.exit_button.pack(pady=5)
        
        # Log frame
        self.log_frame = ttk.LabelFrame(self.main_frame, text="Activity Log", padding=10)
        self.log_frame.pack(fill=tk.BOTH, expand=True)
        
        self.log_text = tk.Text(self.log_frame, height=15, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(self.log_text)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.log_text.yview)
        
        # Initialize variables
        self.running = False
        self.period_date = None
        self.AAcurrent_df = None
        self.AAprevious_df = None
        self.AAStar_df = None
        self.LastReconcile_df = None
        self.GTO05_df = None
        
        # Start checking the message queue
        self.root.after(100, self.process_queue)
    
    def process_queue(self):
        """Check for messages from the worker thread and update GUI"""
        try:
            while True:
                message = self.message_queue.get_nowait()
                if message[0] == 'progress':
                    self.progress['value'] = message[1]
                    self.status_label.config(text=message[2])
                elif message[0] == 'log':
                    timestamp = time.strftime("%H:%M:%S")
                    self.log_text.insert(tk.END, f"[{timestamp}] {message[1]}\n")
                    self.log_text.see(tk.END)
                elif message[0] == 'done':
                    self.running = False
                    self.start_button.config(state=tk.NORMAL)
                    if message[1]:
                        messagebox.showinfo("Success", message[1])
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)
    
    def log_message(self, message):
        """Thread-safe logging"""
        self.message_queue.put(('log', message))
    
    def update_progress(self, value, message):
        """Thread-safe progress update"""
        self.message_queue.put(('progress', value, message))
    
    def start_reconciliation(self):
        if self.running:
            return
            
        self.running = True
        self.start_button.config(state=tk.DISABLED)
        self.log_text.delete(1.0, tk.END)  # Clear log
        
        # Run the reconciliation in a separate thread
        threading.Thread(target=self.run_reconciliation, daemon=True).start()
    
    def run_reconciliation(self):
        try:
            self.update_progress(0, "Starting reconciliation process...")
            
            # Step 1: Get period and files
            self.update_progress(10, "Getting period information...")
            get_input = GetFileAndPeriod(self)
            period_date = get_input.getPeriod()
            
            self.update_progress(20, "Getting input files...")
            AAcurrent_df, AAStar_df, LastReconcile_df, GTO05_df = get_input.getFile()
            
            # Step 2: Process AA data
            self.update_progress(30, "Processing AA data...")
            aa_process = AAProcess(AAcurrent_df, GTO05_df, period_date)
            AAPivot_df, AAForCombine_df = aa_process.getAApivot()
            
            # Step 3: Combine data
            self.update_progress(45, "Combining data...")
            combine = Combine(AAForCombine_df, LastReconcile_df)
            Combined_df = combine.getCombine_df()
            
            # Step 4: Process pending bills
            self.update_progress(60, "Processing pending bills...")
            pendingbills = PendingBills(Combined_df, period_date)
            Pending_df, ForBalance_df = pendingbills.getPendingBills()
            
            # Step 5: Balance reconciliation
            self.update_progress(75, "Balancing reconciliation...")
            balance = Balance(ForBalance_df)
            Balanced_df = balance.getBalance()
            
            # Step 6: Minimum guarantee
            self.update_progress(85, "Calculating minimum guarantee...")
            minimum = Minimum(Balanced_df, AAStar_df, GTO05_df)
            minimum_df = minimum.getMinimum_df()
            MMG_df = minimum.getMMG_df()
            Refund_df = minimum.getRefund_df()
            
            # Step 7: Generate report
            self.update_progress(95, "Generating reports...")
            getReport = GetReport(Pending_df, minimum_df, MMG_df, Refund_df, self)
            getReport.getReport()
            
            self.message_queue.put(('done', "Reconciliation process completed successfully!"))
            self.update_progress(100, "Reconciliation completed successfully!")
            
        except Exception as e:
            # Print error to terminal
            import traceback
            print("\n" + "="*50)
            print("ERROR IN RECONCILIATION PROCESS:")
            traceback.print_exc()
            print("="*50 + "\n")
            
            # Send error to GUI
            error_msg = f"Error: {str(e)}"
            self.log_message(error_msg)
            self.message_queue.put(('done', None))
            self.update_progress(0, "Process failed")
            
            # Show error in GUI messagebox
            self.message_queue.put(('error', "Reconciliation Failed", str(e)))
    
    def process_queue(self):
        """Check for messages from the worker thread and update GUI"""
        try:
            while True:
                message = self.message_queue.get_nowait()
                if message[0] == 'progress':
                    self.progress['value'] = message[1]
                    self.status_label.config(text=message[2])
                elif message[0] == 'log':
                    timestamp = time.strftime("%H:%M:%S")
                    self.log_text.insert(tk.END, f"[{timestamp}] {message[1]}\n")
                    self.log_text.see(tk.END)
                elif message[0] == 'done':
                    self.running = False
                    self.start_button.config(state=tk.NORMAL)
                    if message[1]:
                        messagebox.showinfo("Success", message[1])
                elif message[0] == 'error':
                    self.running = False
                    self.start_button.config(state=tk.NORMAL)
                    messagebox.showerror(message[1], message[2])
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self.process_queue)
    
    def run(self):
        # Center the window on screen
        self.root.eval('tk::PlaceWindow . center')
        self.root.mainloop()

if __name__ == "__main__":
    app = ReconciliationApp()
    app.run()