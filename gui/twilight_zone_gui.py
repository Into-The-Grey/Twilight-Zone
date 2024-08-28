from kivy.app import App
from kivy.uix.boxlayout import BoxLayout


class TwilightZoneApp(App):
    def build(self):
        self.root = BoxLayout()  # Assign the root widget
        return self.root

    def on_button_press(self):
        self.root.ids.status_label.text = "Processing Started!"


if __name__ == "__main__":
    TwilightZoneApp().run()
