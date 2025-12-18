from decimal import Decimal, ROUND_HALF_UP

from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
import pandas as pd


def generate_uk_crypto_tax_pdf_report(df, output_path='uk_crypto_tax_report.pdf',
                                      tax_year_start='2025-04-06', tax_year_end='2026-04-05'):
    # Prepare data
    df['disposal_date'] = pd.to_datetime(df['disposal_date'])
    df = df.sort_values('disposal_date')

    df['profit_in_gbp'] = df['profit_in_gbp']
    df['cost_in_gbp'] = df['cost_in_gbp']
    df['net_profit_in_gbp'] = df['net_profit_in_gbp']




    total_proceeds_in_gbp = df['proceeds_in_gbp'].sum()
    total_cost = df['cost_in_gbp'].sum()
    TWO_PLACES = Decimal("0.01")
    # round first then minus
    total_proceeds_in_gbp = Decimal(str(total_proceeds_in_gbp)).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
    total_cost = Decimal(str(total_cost)).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

    # didn't use df['cost_in_gbp'].sum() as it is different with total_proceeds_in_gbp - total_cost as the reason of round
    total_net_profit = total_proceeds_in_gbp - total_cost

    # Calculate summary statistics
    num_disposals = len(df)
    total_gains = df[df['net_profit_in_gbp'] > 0]['net_profit_in_gbp'].sum()
    total_losses = df[df['net_profit_in_gbp'] < 0]['net_profit_in_gbp'].sum()
    net_gains = total_gains + total_losses  # losses are negative, so this gives net

    # Add acquired date and amount columns if they don't exist
    if 'acquired_date' not in df.columns:
        df['acquired_date'] = df['open_time']  # Use open_time as acquired date
    if 'amount' not in df.columns:
        df['amount'] = df['qty']  # Use qty as amount

    df["acquired_date"] = pd.to_datetime(df["acquired_date"])



    # Update notes column with HMRC rules
    df['notes'] = ''
    for idx, row in df.iterrows():
        acquired_date = pd.to_datetime(row['acquired_date'])
        disposal_date = pd.to_datetime(row['disposal_date'])

        if acquired_date.date() == disposal_date.date():
            df.at[idx, 'notes'] = 'Same Day'
        elif (disposal_date - acquired_date).days <= 30 and (disposal_date - acquired_date).days > 0:
            df.at[idx, 'notes'] = '30-Day Rule'
        # else: leave empty

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

    # Add Report Description (Header Page)
    elements.append(Paragraph(
        "This report includes: Capital Gains, Other Gains, Income, Costs & Expenses, and Gifts, Donations & Lost Coins.",
        styles['Normal']))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("Tax Year", styles['Heading2']))
    elements.append(Paragraph(
        f"{pd.to_datetime(tax_year_start).strftime('%d %b %Y')} to {pd.to_datetime(tax_year_end).strftime('%d %b %Y')}",
        styles['Normal']
    ))
    elements.append(Spacer(1, 16))

    elements.append(Paragraph("Summary of Tax Report", styles['Heading2']))
    elements.append(Spacer(1, 8))

    # Updated Summary Section (Header Page)
    elements.append(Paragraph("Capital Gains:", styles['Heading3']))
    elements.append(Paragraph(f"Number of Disposals: {num_disposals}", styles['Normal']))
    elements.append(Paragraph(f"Disposal Proceeds: {total_proceeds_in_gbp:,.2f} GBP", styles['Normal']))
    elements.append(Paragraph(f"Allowable Costs: {total_cost:,.2f} GBP", styles['Normal']))
    elements.append(Paragraph(f"Gain in This Year: {max(total_gains, 0):,.2f} GBP", styles['Normal']))
    elements.append(Paragraph(f"Losses in This Year: {max(-total_losses, 0):,.2f} GBP", styles['Normal']))
    elements.append(Paragraph(f"Net Gains: {net_gains:,.2f} GBP", styles['Normal']))
    elements.append(Spacer(1, 12))

    # Other Sections (with "No transactions" indication)
    elements.append(Paragraph("Other Gains:", styles['Heading3']))
    elements.append(Paragraph("No transactions", styles['Normal']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Income:", styles['Heading3']))
    elements.append(Paragraph("No transactions", styles['Normal']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Costs & Expenses:", styles['Heading3']))
    elements.append(Paragraph("No transactions", styles['Normal']))
    elements.append(Spacer(1, 6))

    elements.append(Paragraph("Gifts, Donations & Lost Coins:", styles['Heading3']))
    elements.append(Paragraph("No transactions", styles['Normal']))
    elements.append(Spacer(1, 12))

    elements.append(Paragraph(f"Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['SmallGrey']))
    elements.append(PageBreak())

    # === Transaction Table ===
    elements.append(Paragraph("Capital Gains Transactions", styles['Heading2']))  # Renamed section title
    elements.append(Spacer(1, 10))

    fields = ['exchange', 'market', 'disposal_date', 'acquired_date', 'asset', 'amount', 'proceeds_in_gbp',
              'cost_in_gbp', 'net_profit_in_gbp', 'notes']
    table_data = [
        ['#', 'Exchange', 'Market', 'Disposal Date', 'Acquired Date', 'Asset', 'Amount', 'Proceeds\n(GBP)',
         'Cost\n(GBP)', 'Gain/Loss', 'Notes']]

    for idx, row in enumerate(df[fields].itertuples(index=False), start=1):
        # Wrap the notes text in a Paragraph for proper text wrapping
        notes_paragraph = Paragraph(str(row.notes), styles['SmallText'])

        table_data.append([
            str(idx),
            row.exchange,
            row.market,
            row.disposal_date.strftime('%Y-%m-%d\n%H:%M:%S'),
            row.acquired_date.strftime('%Y-%m-%d\n%H:%M:%S'),
            row.asset,
            f"{row.amount:,.4f}",
            f"{row.proceeds_in_gbp:,.2f}",
            f"{row.cost_in_gbp:,.2f}",
            f"{row.net_profit_in_gbp:,.2f}",
            notes_paragraph,
        ])

    # Totals row
    table_data.append([
        '', '', '', '', '', 'Total',
        f"{df['amount'].sum():,.4f}",
        f"{total_proceeds_in_gbp:,.2f}",
        f"{total_cost:,.2f}",
        f"{total_net_profit:,.2f}",
        ''
    ])

    # Define column widths (in points, 72 points = 1 inch)
    # Total available width is approximately 540 points for A4 with margins
    col_widths = [
        18,  # # (Index)
        42,  # Exchange
        42,  # Market
        60,  # Disposal Date
        60,  # Acquired Date
        28,  # Asset
        80,  # Amount
        68,  # Proceeds
        68,  # Cost
        60,  # Gain/Loss
        52,  # Notes (adjusted for new columns)
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
        ('ALIGN', (1, 0), (6, -1), 'LEFT'),  # Text columns left
        ('ALIGN', (7, 1), (9, -1), 'RIGHT'),  # Number columns right
        ('ALIGN', (10, 1), (10, -1), 'LEFT'),  # Notes column left

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
        ('BACKGROUND', (-5, -1), (-1, -1), colors.HexColor("#f0f0f0")),
        ('FONTNAME', (-5, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (-5, -1), (-1, -1), 8),
        ('TEXTCOLOR', (-5, -1), (-1, -1), colors.HexColor("#003366")),

        # Word wrapping for notes column
        ('WORDWRAP', (10, 0), (10, -1), True),
    ]))

    elements.append(table)

    # Build PDF
    doc.build(elements)
    print(f"PDF report saved to: {output_path}")
