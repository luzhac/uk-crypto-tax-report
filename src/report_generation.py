from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
import pandas as pd


def generate_uk_crypto_tax_pdf_report(df,  output_path='uk_crypto_tax_report.pdf',
                                      tax_year_start='2025-04-06', tax_year_end='2026-04-05'):
    # Prepare data
    df['disposal_date'] = pd.to_datetime(df['disposal_date'])
    df = df.sort_values('disposal_date')

    df['profit_in_gbp'] = df['profit_in_gbp'].round(2)
    df['cost_in_gbp'] = df['cost_in_gbp'].round(2)
    df['net_profit_in_gbp'] = df['net_profit_in_gbp'].round(2)

    total_net_profit = df['net_profit_in_gbp'].sum()
    total_cost = df['cost_in_gbp'].sum()

    # Setup PDF
    doc = SimpleDocTemplate(output_path, pagesize=A4,
                            leftMargin=15 * mm, rightMargin=15 * mm,
                            topMargin=20 * mm, bottomMargin=15 * mm)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='SmallGrey', fontSize=8, textColor=colors.grey))
    styles.add(ParagraphStyle(name='BoldRight', fontSize=10, alignment=2, fontName='Helvetica-Bold'))
    styles.add(ParagraphStyle(name='SmallText', fontSize=7, leading=8))  # For table text

    elements = []

    # === Summary Page ===
    elements.append(Paragraph("UK Cryptocurrency Tax Report", styles['Title']))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("Tax Year", styles['Heading2']))
    elements.append(Paragraph(
        f"{pd.to_datetime(tax_year_start).strftime('%d %b %Y')} to {pd.to_datetime(tax_year_end).strftime('%d %b %Y')}",
        styles['Normal']
    ))
    elements.append(Spacer(1, 16))

    elements.append(Paragraph("Summary of Tax Report", styles['Heading2']))
    elements.append(Spacer(1, 8))

    elements.append(Paragraph("Realized Capital Gains", styles['Normal']))
    elements.append(Paragraph(f"{max(total_net_profit, 0):,.2f} GBP", styles['BoldRight']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Realized Capital Losses", styles['Normal']))
    elements.append(Paragraph(f"{max(-total_net_profit, 0):,.2f} GBP", styles['BoldRight']))
    elements.append(Spacer(1, 12))

    elements.append(Paragraph(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['SmallGrey']))
    elements.append(PageBreak())

    # === Transaction Table ===
    elements.append(Paragraph("Transaction Details", styles['Heading2']))
    elements.append(Spacer(1, 10))

    fields = ['exchange', 'market', 'disposal_date', 'asset', 'proceeds_in_gbp', 'cost_in_gbp', 'net_profit_in_gbp',
              'notes']
    table_data = [
        ['#', 'Exchange', 'Market', 'Disposal Date', 'Asset', 'Proceeds\n(GBP)', 'Cost\n(GBP)', 'Gain', 'Notes']]

    for idx, row in enumerate(df[fields].itertuples(index=False), start=1):
        # Wrap the notes text in a Paragraph for proper text wrapping
        notes_paragraph = Paragraph(str(row.notes), styles['SmallText'])

        table_data.append([
            str(idx),
            row.exchange,
            row.market,
            row.disposal_date.strftime('%Y-%m-%d\n%H:%M:%S'),
            row.asset,
            f"{row.proceeds_in_gbp:,.2f}",
            f"{row.cost_in_gbp:,.2f}",
            f"{row.net_profit_in_gbp:,.2f}",
            notes_paragraph,
        ])

    # Totals row
    table_data.append([
        '', '', '', '', 'Total',
        f"{df['proceeds_in_gbp'].sum():,.2f}",
        f"{total_cost:,.2f}",
        f"{df['net_profit_in_gbp'].sum():,.2f}",
        ''
    ])

    # Define column widths (in points, 72 points = 1 inch)
    # Total available width is approximately 540 points for A4 with margins
    col_widths = [
        20,  # # (Index)
        45,  # Exchange
        35,  # Market
        55,  # Date
        30,  # Asset
        50,  # Proceeds
        50,  # Cost
        50,  # Net Profit
        205  # Notes (largest column for full content)
    ]

    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        # Header styling
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#e6e6e6")),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),

        # Data styling
        ('FONTSIZE', (0, 1), (-1, -2), 7),  # All data rows except totals
        ('ALIGN', (0, 0), (0, -1), 'CENTER'),  # Index column center
        ('ALIGN', (1, 0), (4, -1), 'LEFT'),  # Text columns left
        ('ALIGN', (5, 1), (7, -1), 'RIGHT'),  # Number columns right
        ('ALIGN', (8, 1), (8, -1), 'LEFT'),  # Notes column left

        # Vertical alignment
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),

        # Padding
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),

        # Grid lines
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),

        # Totals row styling
        ('BACKGROUND', (-4, -1), (-1, -1), colors.HexColor("#f0f0f0")),
        ('FONTNAME', (-4, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (-4, -1), (-1, -1), 8),
        ('TEXTCOLOR', (-4, -1), (-1, -1), colors.HexColor("#003366")),

        # Word wrapping for notes column
        ('WORDWRAP', (8, 0), (8, -1), True),
    ]))

    elements.append(table)



    # Build PDF
    doc.build(elements)
    print(f"PDF report saved to: {output_path}")