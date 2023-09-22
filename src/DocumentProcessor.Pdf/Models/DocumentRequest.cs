namespace DocumentProcessor.Pdf.Models;

public record DocumentRequest(string RequestId, List<DocumentFile>? Files = null, List<DocumentUrl>? Urls = null)
{
    public List<DocumentFile> Files { get; init; } = Files ?? new List<DocumentFile>();
    public List<DocumentUrl> Urls { get; init; } = Urls ?? new List<DocumentUrl>();
}