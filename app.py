"""
Bluesky Sentiment Analysis - Simple GUI Interface
Uses Tkinter (built into Python - no installation needed!)
"""

import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext, messagebox
import pandas as pd
import re
from collections import Counter
import threading

# Sentiment keywords
POSITIVE_WORDS = [
    'good', 'great', 'awesome', 'amazing', 'excellent', 'wonderful', 'fantastic',
    'love', 'happy', 'best', 'perfect', 'beautiful', 'brilliant', 'nice', 'super',
    'glad', 'enjoy', 'enjoyed', 'thanks', 'thank', 'appreciate', 'excited',
    'fun', 'cool', 'wow', 'yes', 'congrats', 'win'
]

NEGATIVE_WORDS = [
    'bad', 'terrible', 'awful', 'horrible', 'worst', 'hate', 'suck', 'sucks',
    'sad', 'angry', 'mad', 'annoying', 'annoyed', 'disappointed', 'disappointing',
    'wrong', 'failed', 'fail', 'loss', 'lost', 'lose', 'stupid', 'dumb',
    'problem', 'issue', 'broken', 'error', 'never', 'nothing', 'no'
]

class SentimentAnalyzerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("💙 Bluesky Sentiment Analysis")
        self.root.geometry("1000x700")
        self.root.configure(bg='#f0f2f5')
        
        self.df = None
        self.create_widgets()
    
    def create_widgets(self):
        # Header
        header = tk.Frame(self.root, bg='#1DA1F2', height=80)
        header.pack(fill='x')
        
        title = tk.Label(
            header,
            text="💙 Bluesky Sentiment Analysis",
            font=('Arial', 24, 'bold'),
            bg='#1DA1F2',
            fg='white'
        )
        title.pack(pady=20)
        
        # Main container
        main = tk.Frame(self.root, bg='#f0f2f5')
        main.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Left panel - Controls
        left_panel = tk.Frame(main, bg='white', relief='raised', bd=2)
        left_panel.pack(side='left', fill='y', padx=(0, 10))
        
        tk.Label(
            left_panel,
            text="📁 Data File",
            font=('Arial', 14, 'bold'),
            bg='white'
        ).pack(pady=(20, 10), padx=20)
        
        self.file_label = tk.Label(
            left_panel,
            text="No file selected",
            font=('Arial', 10),
            bg='white',
            fg='gray',
            wraplength=200
        )
        self.file_label.pack(pady=5, padx=20)
        
        self.load_btn = tk.Button(
            left_panel,
            text="📂 Load CSV File",
            command=self.load_file,
            bg='#1DA1F2',
            fg='white',
            font=('Arial', 12, 'bold'),
            cursor='hand2',
            relief='flat',
            padx=20,
            pady=10
        )
        self.load_btn.pack(pady=10, padx=20, fill='x')
        
        tk.Label(
            left_panel,
            text="",
            bg='white'
        ).pack(pady=10)
        
        self.analyze_btn = tk.Button(
            left_panel,
            text="🔍 Analyze Sentiment",
            command=self.analyze_sentiment,
            bg='#2ecc71',
            fg='white',
            font=('Arial', 12, 'bold'),
            cursor='hand2',
            relief='flat',
            padx=20,
            pady=10,
            state='disabled'
        )
        self.analyze_btn.pack(pady=10, padx=20, fill='x')
        
        tk.Label(
            left_panel,
            text="",
            bg='white'
        ).pack(pady=10)
        
        self.save_btn = tk.Button(
            left_panel,
            text="💾 Save Results",
            command=self.save_results,
            bg='#9b59b6',
            fg='white',
            font=('Arial', 12, 'bold'),
            cursor='hand2',
            relief='flat',
            padx=20,
            pady=10,
            state='disabled'
        )
        self.save_btn.pack(pady=10, padx=20, fill='x')
        
        # Progress bar
        tk.Label(
            left_panel,
            text="Progress:",
            font=('Arial', 10, 'bold'),
            bg='white'
        ).pack(pady=(30, 5), padx=20)
        
        self.progress = ttk.Progressbar(
            left_panel,
            length=200,
            mode='determinate'
        )
        self.progress.pack(pady=5, padx=20, fill='x')
        
        self.status_label = tk.Label(
            left_panel,
            text="Ready",
            font=('Arial', 9),
            bg='white',
            fg='gray'
        )
        self.status_label.pack(pady=5, padx=20)
        
        # Right panel - Results
        right_panel = tk.Frame(main, bg='white', relief='raised', bd=2)
        right_panel.pack(side='right', fill='both', expand=True)
        
        # Notebook for tabs
        self.notebook = ttk.Notebook(right_panel)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Tab 1: Statistics
        stats_frame = tk.Frame(self.notebook, bg='white')
        self.notebook.add(stats_frame, text='📊 Statistics')
        
        self.stats_text = scrolledtext.ScrolledText(
            stats_frame,
            font=('Courier', 11),
            bg='#f8f9fa',
            relief='flat',
            padx=20,
            pady=20
        )
        self.stats_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Tab 2: Positive Posts
        positive_frame = tk.Frame(self.notebook, bg='white')
        self.notebook.add(positive_frame, text='🟢 Positive')
        
        self.positive_text = scrolledtext.ScrolledText(
            positive_frame,
            font=('Arial', 10),
            bg='#d4edda',
            relief='flat',
            padx=20,
            pady=20
        )
        self.positive_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Tab 3: Negative Posts
        negative_frame = tk.Frame(self.notebook, bg='white')
        self.notebook.add(negative_frame, text='🔴 Negative')
        
        self.negative_text = scrolledtext.ScrolledText(
            negative_frame,
            font=('Arial', 10),
            bg='#f8d7da',
            relief='flat',
            padx=20,
            pady=20
        )
        self.negative_text.pack(fill='both', expand=True, padx=10, pady=10)
        
        # Tab 4: Word Analysis
        words_frame = tk.Frame(self.notebook, bg='white')
        self.notebook.add(words_frame, text='📝 Words')
        
        self.words_text = scrolledtext.ScrolledText(
            words_frame,
            font=('Courier', 10),
            bg='#f8f9fa',
            relief='flat',
            padx=20,
            pady=20
        )
        self.words_text.pack(fill='both', expand=True, padx=10, pady=10)
    
    def load_file(self):
        """Load CSV file"""
        filename = filedialog.askopenfilename(
            title="Select CSV file",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                self.df = pd.read_csv(filename)
                self.file_label.config(
                    text=f"✅ Loaded: {len(self.df):,} posts",
                    fg='green'
                )
                self.analyze_btn.config(state='normal')
                self.status_label.config(text="File loaded successfully!")
                
                messagebox.showinfo(
                    "Success",
                    f"Loaded {len(self.df):,} posts successfully!"
                )
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load file:\n{e}")
                self.file_label.config(text="❌ Load failed", fg='red')
    
    def clean_text(self, text):
        """Clean text"""
        if pd.isna(text) or text == '':
            return ""
        
        text = str(text).lower()
        text = re.sub(r'http[s]?://\S+', '', text)
        text = re.sub(r'@\w+', '', text)
        text = text.replace('#', '')
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = ' '.join(text.split())
        
        return text
    
    def analyze_sentiment_row(self, text):
        """Analyze sentiment for one text"""
        if not text or len(text) < 3:
            return 0, 'Neutral'
        
        words = text.lower().split()
        
        positive_count = sum(1 for word in words if word in POSITIVE_WORDS)
        negative_count = sum(1 for word in words if word in NEGATIVE_WORDS)
        
        score = positive_count - negative_count
        
        if score > 0:
            sentiment = 'Positive'
        elif score < 0:
            sentiment = 'Negative'
        else:
            sentiment = 'Neutral'
        
        return score, sentiment
    
    def analyze_sentiment(self):
        """Analyze sentiment in thread"""
        self.analyze_btn.config(state='disabled')
        self.progress['value'] = 0
        self.status_label.config(text="Analyzing...")
        
        thread = threading.Thread(target=self._perform_analysis)
        thread.start()
    
    def _perform_analysis(self):
        """Perform the actual analysis"""
        try:
            # Clean text
            self.progress['value'] = 20
            self.df['clean_text'] = self.df['text'].apply(self.clean_text)
            
            # Analyze sentiment
            self.progress['value'] = 50
            results = self.df['clean_text'].apply(self.analyze_sentiment_row)
            self.df[['sentiment_score', 'sentiment']] = pd.DataFrame(
                results.tolist(), index=self.df.index
            )
            
            self.progress['value'] = 80
            
            # Display results
            self.root.after(0, self.display_results)
            
            self.progress['value'] = 100
            self.status_label.config(text="Analysis complete!")
            self.save_btn.config(state='normal')
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Error", f"Analysis failed:\n{e}"))
            self.status_label.config(text="Analysis failed!")
    
    def display_results(self):
        """Display analysis results"""
        # Calculate stats
        total = len(self.df)
        positive = len(self.df[self.df['sentiment'] == 'Positive'])
        negative = len(self.df[self.df['sentiment'] == 'Negative'])
        neutral = len(self.df[self.df['sentiment'] == 'Neutral'])
        
        # Statistics tab
        self.stats_text.delete(1.0, tk.END)
        stats = f"""
{'='*60}
           SENTIMENT ANALYSIS RESULTS
{'='*60}

📊 OVERALL STATISTICS
{'─'*60}
Total Posts Analyzed:  {total:,}

🟢 Positive:  {positive:,} posts  ({positive/total*100:.1f}%)
🔴 Negative:  {negative:,} posts  ({negative/total*100:.1f}%)
⚪ Neutral:   {neutral:,} posts  ({neutral/total*100:.1f}%)

Average Sentiment Score: {self.df['sentiment_score'].mean():.2f}

{'='*60}

📊 SENTIMENT BY LANGUAGE
{'─'*60}
"""
        self.stats_text.insert(1.0, stats)
        
        # Language breakdown
        lang_stats = self.df.groupby(['lang', 'sentiment']).size().unstack(fill_value=0)
        self.stats_text.insert(tk.END, str(lang_stats))
        self.stats_text.insert(tk.END, f"\n\n{'='*60}\n")
        
        # Positive posts tab
        self.positive_text.delete(1.0, tk.END)
        self.positive_text.insert(1.0, "🌟 TOP 10 MOST POSITIVE POSTS\n")
        self.positive_text.insert(tk.END, "="*60 + "\n\n")
        
        top_positive = self.df[self.df['sentiment'] == 'Positive'].nlargest(10, 'sentiment_score')
        for i, (idx, row) in enumerate(top_positive.iterrows(), 1):
            post = f"{i}. Score: {row['sentiment_score']} | Language: {row['lang']}\n"
            post += f"   {row['text'][:200]}...\n\n"
            post += "─"*60 + "\n\n"
            self.positive_text.insert(tk.END, post)
        
        # Negative posts tab
        self.negative_text.delete(1.0, tk.END)
        self.negative_text.insert(1.0, "😞 TOP 10 MOST NEGATIVE POSTS\n")
        self.negative_text.insert(tk.END, "="*60 + "\n\n")
        
        top_negative = self.df[self.df['sentiment'] == 'Negative'].nsmallest(10, 'sentiment_score')
        for i, (idx, row) in enumerate(top_negative.iterrows(), 1):
            post = f"{i}. Score: {row['sentiment_score']} | Language: {row['lang']}\n"
            post += f"   {row['text'][:200]}...\n\n"
            post += "─"*60 + "\n\n"
            self.negative_text.insert(tk.END, post)
        
        # Word analysis tab
        self.words_text.delete(1.0, tk.END)
        self.words_text.insert(1.0, "📝 MOST COMMON WORDS\n")
        self.words_text.insert(tk.END, "="*60 + "\n\n")
        
        all_words = ' '.join(self.df['clean_text']).split()
        word_counts = Counter(all_words).most_common(30)
        
        for i, (word, count) in enumerate(word_counts, 1):
            self.words_text.insert(tk.END, f"{i:2}. {word:20} {count:5} {'█' * (count // 10)}\n")
    
    def save_results(self):
        """Save results to CSV"""
        filename = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        
        if filename:
            try:
                output_df = self.df[[
                    '_id', 'did', 'text', 'clean_text', 'lang', 
                    'created_at', 'sentiment', 'sentiment_score'
                ]]
                output_df.to_csv(filename, index=False)
                messagebox.showinfo("Success", f"Results saved to:\n{filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save:\n{e}")

# Run the application
if __name__ == "__main__":
    root = tk.Tk()
    app = SentimentAnalyzerGUI(root)
    root.mainloop()
