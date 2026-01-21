import os
import cv2
import pandas as pd
from ultralytics import YOLO
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    filename='logs/yolo_detect.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def detect_objects(source_dir='data/raw/images', output_csv='data/processed/yolo_detections.csv'):
    # Initialize YOLO model (using nano version for speed)
    model = YOLO('yolov8n.pt')
    
    detections = []
    
    source_path = Path(source_dir)
    if not source_path.exists():
        logging.error(f"Source directory {source_dir} does not exist.")
        return

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    for channel_dir in source_path.iterdir():
        if channel_dir.is_dir():
            channel_name = channel_dir.name
            logging.info(f"Processing channel: {channel_name}")
            
            for image_file in channel_dir.glob('*.jpg'):
                try:
                    message_id = image_file.stem
                    image_path = str(image_file)
                    
                    # Run inference
                    results = model(image_path, verbose=False)
                    
                    # Process results
                    result = results[0]
                    
                    # Collect detected classes and confidences
                    detected_classes = []
                    confidences = []
                    boxes = [] # xyxy
                    
                    for box in result.boxes:
                        cls_id = int(box.cls[0])
                        cls_name = model.names[cls_id]
                        conf = float(box.conf[0])
                        xyxy = box.xyxy[0].tolist()
                        
                        detected_classes.append(cls_name)
                        confidences.append(conf)
                        boxes.append(xyxy)
                        
                        # Add individual detection row (optional, depending on granularity needed)
                        # For now, let's keep one row per image with lists/summary, 
                        # OR one row per detection.
                        # The task implies "Integrate with Data Warehouse", let's do one row per detection for maximum flexibility in SQL.
                        
                        detections.append({
                            'image_path': image_path,
                            'channel_name': channel_name,
                            'message_id': message_id,
                            'label': cls_name,
                            'confidence': conf,
                            'x_min': xyxy[0],
                            'y_min': xyxy[1],
                            'x_max': xyxy[2],
                            'y_max': xyxy[3]
                        })
                        
                except Exception as e:
                    logging.error(f"Error processing {image_file}: {e}")

    # Save to CSV
    if detections:
        df = pd.DataFrame(detections)
        df.to_csv(output_csv, index=False)
        logging.info(f"Saved {len(detections)} detections to {output_csv}")
        print(f"Saved {len(detections)} detections to {output_csv}")
    else:
        logging.warning("No detections found.")
        print("No detections found.")

if __name__ == '__main__':
    detect_objects()