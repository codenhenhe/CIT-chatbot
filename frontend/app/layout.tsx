import type { Metadata } from "next";
import { Inter, Geist_Mono } from "next/font/google";
import "./globals.css";

// Cấu hình font Inter
const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin", "vietnamese"], // Thêm vietnamese để không bị lỗi dấu
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "CICTBot - Trợ lý ảo Văn phòng Trường CNTT-TT",
  description: "Trợ lý ảo trích xuất và trả lời thông tin văn bản cho văn phòng trường CNTT-TT",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi">
      <body
        // Sử dụng inter.variable và class font-sans
        className={`${inter.variable} ${geistMono.variable} font-sans antialiased`}
      >
        {children}
      </body>
    </html>
  );
}