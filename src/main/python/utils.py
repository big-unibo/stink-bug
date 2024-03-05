from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib import rc

def savePdf(name, fig) :
    """
    Save the figure to a pdf file
    :param name: the figure name without the ".pdf" extension
    :param fig: the figure to save
    :return: Nothing, the figure is saved to a pdf file in the specified name
    """
    pp = PdfPages(name + '.pdf')
    pp.savefig( fig, dpi=300, bbox_inches = "tight" )
    pp.close()

rc('text', usetex=True)
def set_font_size(size) :
    """
    Set the font size
    :param size: the size of the font
    :return: None
    """
    rc_fonts = {
        "text.usetex": True,
        "font.size": size,
        'mathtext.default': 'regular',
        'axes.titlesize': size,
        "axes.labelsize": size,
        "legend.fontsize": size,
        "xtick.labelsize": size,
        "ytick.labelsize": size,
        'figure.titlesize': size,
        #'text.latex.preamble': [r'\usepackage{amsmath,amssymb,bm,physics,lmodern}'],
        "font.family": "serif",
        "font.serif": "computer modern roman",
    }
    plt.rcParams.update(rc_fonts)