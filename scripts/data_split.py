import pandas as pd
import os

def split_dataset(input_file: str, output_folder: str, num_files: int):
    
    # Read the dataset
    df = pd.read_csv(input_file)
    
    # Calculate rows per file
    total_rows = len(df)
    rows_per_file = total_rows // num_files

    # Split and save files
    for i in range(num_files):
        start_idx = i * rows_per_file
        end_idx = start_idx + rows_per_file if i < num_files - 1 else total_rows
        
        # Get subset of data
        subset = df.iloc[start_idx:end_idx].copy()
        
        # Save to file
        output_file = os.path.join(output_folder, f'chunk_{i}.csv')
        subset.to_csv(output_file, index=False)
        print(f"Saved: {output_file}")

if __name__ == "__main__":
    split_dataset (
        
        input_file="C:\\Users\HP\\Downloads\\jblink_dsp_project\\JB_dataset\\raw_data_with_issues\\main_raw_data_with_issues.csv",
        output_folder="C:\\Users\\HP\\Downloads\\jblink_dsp_project\\data_ingestion\\raw_data",
        num_files=700
        
        )