from flirimageextractor import FlirImageExtractor
 
image_path = r"C:\Users\st1mohamna\OneDrive - Prometeon\Desktop\IgniteSync\100_FLIR\FLIR0007.jpg"
 
try:
    fie = FlirImageExtractor()
    fie.process_image(image_path)
 
    # Extract temperature array
    thermal = fie.get_thermal_np()
 
    max_temp = thermal.max()
    avg_temp = thermal.mean()
 
    print(f"Max Temperature: {max_temp:.2f} °C")
    print(f"Average Temperature: {avg_temp:.2f} °C")
 
except Exception as e:
    print("Error:", e)