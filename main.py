#!/usr/bin/env python3

"""
Extract JSON data from CSV files where JSON is embedded in fields.
Can extract a specific row or all rows to separate JSON files.
"""

import csv
import json
import sys
from pathlib import Path

def analyze_csv_structure(file_path, show_chars=500):
	print("Analyzing CSV structure...")
	print("=" * 60)
	with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
		lines = f.readlines()
		print(f"Total lines in file: {len(lines)}")
		for i, line in enumerate(lines[:min(10, len(lines))], 1):
			print(f"Line {i}: {len(line):,} chars")
			if len(line) > show_chars:
				print(f"  Preview: {line[:show_chars]}...")
			else:
				print(f"  Preview: {line.strip()}")
		if len(lines) > 10:
			print(f"... and {len(lines) - 10} more lines")
	return len(lines)


def extract_single_row(file_path, output_path, row_number):
	print(f"\nExtracting row {row_number}...")
	print("-" * 60)
	try:
		with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
			csv.field_size_limit(sys.maxsize)
			# try to detect dialect
			sample = f.read(10000)
			f.seek(0)
			try:
				dialect = csv.Sniffer().sniff(sample)
			except:
				dialect = csv.excel()
			reader = csv.reader(f, dialect=dialect)
			
			# skip to the target row
			for current_row_num, row in enumerate(reader, 1):
				if current_row_num == row_number:
					if not row:
						print(f"✗ Row {row_number} is empty")
						return False
					
					# find the JSON column (usually the longest)
					json_column = max(row, key=len) if row else ""
					
					if not json_column:
						print(f"✗ No data found in row {row_number}")
						return False
					
					print(f"Found data: {len(json_column):,} chars")
					
					# save the extracted JSON
					with open(output_path, 'w', encoding='utf-8') as out:
						out.write(json_column)
					
					print(f"✓ Saved to {output_path}")
					
					# quick validation
					try:
						if json_column.strip().startswith('[') or json_column.strip().startswith('{'):
							# test parse a sample
							test_sample = json_column[:1000]
							if json_column.strip().startswith('['):
								test_sample = test_sample + ']'
							else:
								test_sample = test_sample + '}'
							json.loads(test_sample)
							print("✓ JSON structure appears valid")
						else:
							print("⚠ Data doesn't start with [ or { - might not be JSON")
					except:
						print("⚠ JSON validation failed - may need cleaning")
					
					return True
			
			print(f"✗ File only has {current_row_num} rows, cannot extract row {row_number}")
			return False
			
	except Exception as e:
		print(f"✗ Error extracting row: {e}")
		return False


def extract_all_rows(file_path, output_prefix):
	print("\nExtracting all rows to separate JSON files...")
	print("-" * 60)
	output_dir = Path(output_prefix).parent
	base_name = Path(output_prefix).stem
	try:
		with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
			csv.field_size_limit(sys.maxsize)
			# try to detect dialect
			sample = f.read(10000)
			f.seek(0)
			try:
				dialect = csv.Sniffer().sniff(sample)
			except:
				dialect = csv.excel()
			reader = csv.reader(f, dialect=dialect)
			
			extracted_count = 0
			skipped_count = 0
			for row_number, row in enumerate(reader, 1):
				if not row:
					print(f"Row {row_number}: Empty - skipping")
					skipped_count += 1
					continue
				
				# find the JSON column (usually the longest)
				json_column = max(row, key=len) if row else ""
				if not json_column or len(json_column.strip()) < 2:
					print(f"Row {row_number}: No significant data - skipping")
					skipped_count += 1
					continue
				
				output_file = output_dir / f"{base_name}_row_{row_number:03d}.json"
				with open(output_file, 'w', encoding='utf-8') as out:
					out.write(json_column)
				
				file_size = len(json_column)
				print(f"Row {row_number}: {file_size:,} chars -> {output_file.name}")
				
				is_valid = False
				if json_column.strip().startswith('[') or json_column.strip().startswith('{'):
					try:
						test_sample = json_column[:1000]
						if json_column.strip().startswith('['):
							test_sample = test_sample + ']'
						else:
							test_sample = test_sample + '}'
						json.loads(test_sample)
						is_valid = True
					except:
						pass
				
				if is_valid:
					print(f"  ✓ Valid JSON structure")
				else:
					print(f"  ⚠ May not be valid JSON")
				
				extracted_count += 1
			
			print("-" * 60)
			print(f"✓ Extracted {extracted_count} rows")
			if skipped_count > 0:
				print(f"  Skipped {skipped_count} empty/invalid rows")
			
			return extracted_count > 0
			
	except Exception as e:
		print(f"✗ Error extracting rows: {e}")
		return False


def validate_json_file(json_file):
	print(f"\nValidating {json_file}...")
	
	try:
		with open(json_file, 'r', encoding='utf-8') as f:
			content = f.read()
			
		if not content.strip():
			print("  ✗ File is empty")
			return False
		
		first_char = content.strip()[0]
		last_char = content.strip()[-1]
		
		if first_char not in '[{':
			print(f"  ⚠ Doesn't start with [ or {{ (starts with '{first_char}')")
			return False
		
		if last_char not in ']}':
			print(f"  ⚠ Doesn't end with ] or }} (ends with '{last_char}')")
		
		try:
			json.loads(content)
			print("  ✓ Valid JSON!")
			return True
		except json.JSONDecodeError as e:
			print(f"  ✗ JSON parse error: {e}")
			
			# Check bracket balance
			open_braces = content.count('{') - content.count('}')
			open_brackets = content.count('[') - content.count(']')
			
			if open_braces != 0:
				print(f"    Unmatched braces: {open_braces:+d}")
			if open_brackets != 0:
				print(f"    Unmatched brackets: {open_brackets:+d}")
			
			return False
			
	except Exception as e:
		print(f"  ✗ Error reading file: {e}")
		return False


def main():
	if len(sys.argv) < 3:
		print("Usage:")
		print("  Extract specific row:  python3 extract_json.py input.csv output.json [row_number]")
		print("  Extract all rows:      python3 extract_json.py input.csv output_prefix")
		print("")
		print("Examples:")
		print("  python3 extract_json.py data.csv extracted.json 4    # Extract row 4")
		print("  python3 extract_json.py data.csv output/data         # Extract all rows to output/data_row_001.json, etc.")
		print("")
		print("Note: If no row number is specified, all rows will be extracted to separate files")
		sys.exit(1)
	
	input_file = sys.argv[1]
	output_path = sys.argv[2]
	
	if not Path(input_file).exists():
		print(f"Error: {input_file} not found")
		sys.exit(1)
	
	total_lines = analyze_csv_structure(input_file)
	print("=" * 60)
	
	# check if a specific row was requested
	if len(sys.argv) > 3:
		try:
			row_number = int(sys.argv[3])
			if row_number < 1:
				print("Error: Row number must be positive")
				sys.exit(1)
			if row_number > total_lines:
				print(f"Error: Row {row_number} requested but file only has {total_lines} lines")
				sys.exit(1)
			success = extract_single_row(input_file, output_path, row_number)
			if success:
				print("\n" + "=" * 60)
				print("Extraction complete!")
				validate_json_file(output_path)
				print("\nNext steps:")
				print(f"  1. View: head -c 1000 {output_path}")
				print(f"  2. Validate: python3 -m json.tool {output_path} > validated.json")
				print(f"  3. If invalid: python3 fix_json.py {output_path} fixed.json")
			else:
				sys.exit(1)
				
		except ValueError:
			print(f"Error: '{sys.argv[3]}' is not a valid row number")
			sys.exit(1)
	else:
		print("No row number specified - extracting all rows...")
		
		output_dir = Path(output_path).parent
		if output_dir and not output_dir.exists():
			output_dir.mkdir(parents=True, exist_ok=True)
			print(f"Created directory: {output_dir}")
		success = extract_all_rows(input_file, output_path)
		
		if success:
			print("\n" + "=" * 60)
			print("Extraction complete!")
			print("\nTo validate all files:")
			
			base_name = Path(output_path).stem
			output_pattern = str(Path(output_path).parent / f"{base_name}_row_*.json")
			
			print(f"  for file in {output_pattern}; do")
			print(f"    echo \"Checking $file...\"")
			print(f"    python3 -m json.tool \"$file\" > /dev/null && echo \"  ✓ Valid\" || echo \"  ✗ Invalid\"")
			print(f"  done")
		else:
			sys.exit(1)


if __name__ == "__main__":
	main()


