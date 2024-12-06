import streamlit as st
import boto3
import streamlit.components.v1 as components

class FlightDashboard:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket = 'flight-delay-analytics'
        self.folder = 'presentation'
        self.image_descriptions = {
            "flight_cancellations.png": "Pattern of flight cancellations across different time periods.",
            "flight_delay_distribution_1_60_min.png": "Distribution of flight delays between 1-60 minutes.",
            "flight_delay_distribution.png": "Full spectrum of flight delays.",
            "flight_results_delaygroup.png": "Analysis by delay groups.",
            "number_of_flights.png": "Total flight volume over time.",
            "Scheduled_Flights_Per_Year.png": "Annual scheduled flights breakdown."
        }

    def load_html(self, file_name):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=f'{self.folder}/{file_name}')
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            st.error(f"Error loading HTML: {str(e)}")
            return None

    def load_image(self, image_name):
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=f'{self.folder}/{image_name}')
            return response['Body'].read()
        except Exception as e:
            st.error(f"Error loading image: {str(e)}")
            return None

    def display_html(self):
        html_content = self.load_html("interactive_flight_cancellations.html")
        if html_content:
            st.markdown("### Interactive Flight Cancellations")
            st.components.v1.html(html_content, height=800, scrolling=True)
        else:
            st.error("Failed to load visualization")

    def display_images(self):
        for image_file, description in self.image_descriptions.items():
            st.header(image_file.replace('_', ' ').replace('.png', ''))
            st.markdown(description)
            img_data = self.load_image(image_file)
            if img_data:
                st.image(img_data, use_container_width=True)
            st.markdown("---")

    def run(self):
        st.set_page_config(layout="wide", page_title="Flight Analysis Dashboard")
        
        page = st.sidebar.radio("View", ["Interactive", "Static Images"])
        st.title("Flight Analysis Dashboard")
        
        if page == "Interactive":
            self.display_html()
        else:
            self.display_images()

if __name__ == "__main__":
    dashboard = FlightDashboard()
    dashboard.run()